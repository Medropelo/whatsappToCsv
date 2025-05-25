import re
import pandas as pd
from datetime import datetime
import uuid

WHATSAPP_CHAT_FILE = '/content/drive/MyDrive/bonga-wpp/WhatsApp Chat with Bonga.txt'  # <--- IMPORTANT: Change to your uploaded chat file name

# --- 2. Regular Expressions for Parsing ---
# This regex tries to handle common WhatsApp export formats (DD/MM/YY, HH:MM or MM/DD/YYYY, HH:MM AM/PM).
# Group 'date', 'time', 'user', 'message'
MESSAGE_LINE_REGEX = re.compile(
    r"^(?P<date>\d{1,2}/\d{1,2}/\d{2}),\s*(?P<time>\d{1,2}:\d{2}(?:\s*(?:[AP]M))?)\s*-\s*(?P<user>[^:]+):\s*(?P<message>.+)",
    re.IGNORECASE
)
# Regex for common media messages (adapt if yours are different)
MEDIA_MESSAGE_REGEX = re.compile(r"^<Media omitted>$|^.+\(file attached\)$|^\s*sticker omitted\s*$", re.IGNORECASE)
# Regex for some common system messages (you might need to add more)
# These typically don't have the "User Name:" part
SYSTEM_MESSAGE_PATTERNS = [
    re.compile(r".* created group " ),
    re.compile(r".* added .*"),
    re.compile(r".* left$"),
    re.compile(r".* changed this group's icon"),
    re.compile(r".* changed the subject from .*"),
    re.compile(r".* changed their phone number .*"),
    re.compile(r"Messages and calls are end-to-end encrypted.*"),
    re.compile(r".* changed the group description"),
    re.compile(r".*was added$"),
    re.compile(r".*You're now an admin$")
]

# --- 3. Parsing Function ---
def parse_whatsapp_chat(file_path):
    """
    Parses a WhatsApp chat export file.
    """
    messages_data = []
    current_message_bundle = None
    unparsed_lines_count = 0

    print(f"Starting to parse '{file_path}'...")
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            match = MESSAGE_LINE_REGEX.match(line)
            is_system_msg_type1 = False # System messages that fit the "User: Message" structure but are system
            is_system_msg_type2 = any(pattern.match(line) for pattern in SYSTEM_MESSAGE_PATTERNS) # System messages that don't fit

            if match:
                if current_message_bundle: # Store previous message if new one starts
                    messages_data.append(current_message_bundle)

                date_str = match.group('date')
                time_str = match.group('time')
                user_name_raw = match.group('user').strip()
                message_text_raw = match.group('message').strip()

                # Try to parse datetime from various common formats
                dt_obj = None
                full_datetime_str = f"{date_str}, {time_str.upper()}" # upper() for AM/PM
                possible_formats = [
                    "%d/%m/%Y, %H:%M", "%m/%d/%y, %H:%M", # <-- %m/%d/%y might be an issue if it's strictly dd/MM/yy
                    "%d/%m/%y, %H:%M", # <-- This one matches dd/MM/yy, HH:MM
                    "%m/%d/%Y, %H:%M",
                    "%d/%m/%Y, %I:%M %p", "%m/%d/%y, %I:%M %p", # <-- and this for AM/PM
                    "%d/%m/%y, %I:%M %p", # <-- This one matches dd/MM/yy, hh:MM AM/PM
                    "%m/%d/%Y, %I:%M %p"
                ]
                for fmt in possible_formats:
                    try:
                        dt_obj = datetime.strptime(full_datetime_str, fmt)
                        break
                    except ValueError:
                        continue

                if not dt_obj:
                    print(f"Warning: Could not parse date/time at line {line_num}: '{full_datetime_str}'. Treating as part of previous or unparsed.")
                    # If there's an existing bundle, append this problematic line to it
                    if current_message_bundle:
                         current_message_bundle['message_text'] += "\n" + line
                         current_message_bundle['raw_line'] += "\n" + line
                    else: # Or treat as a new unparsed system line
                        messages_data.append({
                            'message_id': str(uuid.uuid4()),
                            'message_timestamp': pd.NaT, # Null timestamp
                            'date_partition': None,
                            'user_name': "SYSTEM_UNPARSED",
                            'message_text': line,
                            'is_media': False,
                            'is_system_message': True,
                            'raw_line': line
                        })
                    unparsed_lines_count +=1
                    continue

                # Check if the user_name itself indicates a system message
                # (e.g. if a system message accidentally matched the user part)
                if any(pattern.match(user_name_raw + ": " + message_text_raw) for pattern in SYSTEM_MESSAGE_PATTERNS):
                    is_system_msg_type1 = True
                    message_text_raw = user_name_raw + ": " + message_text_raw # Reconstruct full message
                    user_name_raw = "SYSTEM" # Assign a generic user for these


                is_media = bool(MEDIA_MESSAGE_REGEX.match(message_text_raw))

                current_message_bundle = {
                    'message_id': str(uuid.uuid4()),
                    'message_timestamp': dt_obj,
                    'date_partition': dt_obj.date(),
                    'user_name': user_name_raw,
                    'message_text': message_text_raw,
                    'is_media': is_media,
                    'is_system_message': is_system_msg_type1,
                    'raw_line': line
                }

            elif is_system_msg_type2: # Handle system messages that don't match the main regex
                if current_message_bundle: # Store previous message
                    messages_data.append(current_message_bundle)
                    current_message_bundle = None

                # Try to extract a timestamp if the system message is timestamped by WhatsApp
                # (This is a simplified check, real system messages can be complex)
                system_dt_match = re.match(r"^(?P<date>\d{1,2}/\d{1,2}/\d{2,4}),\s*(?P<time>\d{1,2}:\d{2}(?:\s*(?:AM|PM))?)\s*-\s*(?P<text_payload>.+)", line)
                dt_obj_system = pd.NaT
                date_part_system = None
                user_system = "SYSTEM"
                text_system = line

                if system_dt_match:
                    try:
                        full_dt_str = f"{system_dt_match.group('date')}, {system_dt_match.group('time').upper()}"
                        for fmt in possible_formats:
                            try:
                                dt_obj_system = datetime.strptime(full_dt_str, fmt)
                                date_part_system = dt_obj_system.date()
                                text_system = system_dt_match.group('text_payload') # The actual system message content
                                break
                            except ValueError:
                                continue
                    except Exception: # Catch any error during system date parsing
                        pass # Keep dt_obj_system as NaT

                messages_data.append({
                    'message_id': str(uuid.uuid4()),
                    'message_timestamp': dt_obj_system,
                    'date_partition': date_part_system,
                    'user_name': user_system,
                    'message_text': text_system,
                    'is_media': False,
                    'is_system_message': True,
                    'raw_line': line
                })

            elif current_message_bundle: # This is a continuation of the previous message (multi-line)
                current_message_bundle['message_text'] += "\n" + line
                current_message_bundle['raw_line'] += "\n" + line
            else:
                # Line doesn't match and there's no current message (e.g., header, very first line if not a msg)
                print(f"Info: Line {line_num} is an unhandled starting line: '{line}'")
                messages_data.append({
                    'message_id': str(uuid.uuid4()),
                    'message_timestamp': pd.NaT,
                    'date_partition': None,
                    'user_name': "SYSTEM_UNHANDLED_START",
                    'message_text': line,
                    'is_media': False,
                    'is_system_message': True,
                    'raw_line': line
                })
                unparsed_lines_count +=1


    # Add the last message bundle
    if current_message_bundle:
        messages_data.append(current_message_bundle)

    print(f"Finished parsing. Total messages/entries generated: {len(messages_data)}.")
    if unparsed_lines_count > 0:
        print(f"Warning: Encountered {unparsed_lines_count} lines that were treated as unparsed or had date parsing issues.")
    return messages_data

# --- 4. DataFrame Creation ---
def create_dataframe(parsed_messages):
    """Converts parsed messages to a Pandas DataFrame with appropriate types."""
    if not parsed_messages:
        print("No messages parsed, returning empty DataFrame.")
        return pd.DataFrame()

    df = pd.DataFrame(parsed_messages)

    # Convert to proper dtypes
    # For message_timestamp, ensure it's datetime and UTC for BigQuery
    df['message_timestamp'] = pd.to_datetime(df['message_timestamp'], errors='coerce')
    if df['message_timestamp'].dt.tz is None:
      df['message_timestamp'] = df['message_timestamp'].dt.tz_localize('UTC') # Assume UTC if naive, or convert
    else:
      df['message_timestamp'] = df['message_timestamp'].dt.tz_convert('UTC')

    # date_partition should be a date object (Pandas might make it datetime, BQ client handles it)
    df['date_partition'] = pd.to_datetime(df['date_partition'], errors='coerce').dt.date

    df['is_media'] = df['is_media'].astype(bool)
    df['is_system_message'] = df['is_system_message'].astype(bool)
    df['user_name'] = df['user_name'].astype(str)
    df['message_text'] = df['message_text'].astype(str)
    df['raw_line'] = df['raw_line'].astype(str)
    df['message_id'] = df['message_id'].astype(str)

    # Drop rows where critical info might be missing after conversion (e.g. timestamp essential for partitioning)
    # df.dropna(subset=['message_timestamp', 'date_partition'], inplace=True) # Be careful with this

    print("DataFrame created with dtypes:")
    print(df.dtypes)
    print(f"DataFrame shape: {df.shape}")
    return df

#Save DF as CSV
def save_df(df, file_path):
    df.to_csv(file_path, index=False)
    print(f"DataFrame saved to {file_path}")

df = parse_whatsapp_chat(WHATSAPP_CHAT_FILE)
df = create_dataframe(df)
save_df(df, 'wpp_messages.csv')
