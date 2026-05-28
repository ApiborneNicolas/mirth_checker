#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Mirth Log Parser
================
This script provides functions to parse, decode, and filter Mirth log files.
It can also be executed as a command-line tool to print statistics about a log file.

Functions:
- mirth_file_parser(filename) -> list[str]
- mirth_log_decoder(log_entry_str) -> dict
- mirth_log_filter(log_entries, search_terms) -> list
"""

import os
import re


def mirth_file_parser(filename):
    """
    Parses a Mirth log file and groups lines belonging to the same log entry.
    A new log entry starts with a line containing the log level and the timestamp.
    
    Args:
        filename (str): Path to the log file.
        
    Returns:
        list[str]: A list of log entries, where each entry is a string (potentially multiline).
    """
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Log file not found: {filename}")
        
    entries = []
    current_entry = []
    
    # Use utf-8 with replacement for invalid characters, fallback to latin-1 if needed
    try:
        with open(filename, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
    except Exception:
        with open(filename, 'r', encoding='latin-1', errors='replace') as f:
            lines = f.readlines()
            
    # Pattern matching log entry start: e.g. ERROR 2026-05-06 15:35:57.200 or INFO  2026-05-06 15:36:07.275
    # Standard format: LEVEL YYYY-MM-DD HH:MM:SS.SSS
    header_pattern = re.compile(r'^[A-Z]+\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}')
    
    for line in lines:
        if header_pattern.match(line):
            if current_entry:
                entries.append("".join(current_entry))
                current_entry = []
            current_entry.append(line)
        else:
            if current_entry:
                current_entry.append(line)
            else:
                # If file starts with lines that have no header, group them as the first entry
                current_entry.append(line)
                
    if current_entry:
        entries.append("".join(current_entry))
        
    return entries

def mirth_log_decoder(log_entry_str):
    """
    Decodes a single log entry string into a dictionary of structured information.
    
    Args:
        log_entry_str (str): The log entry string (can be multiline).
        
    Returns:
        dict: A dictionary containing:
            - type (str): Log level (INFO, ERROR, WARN, DEBUG, etc.) or None
            - datetime (str): Log timestamp (YYYY-MM-DD HH:MM:SS.SSS) or None
            - thread (str): Thread name or None
            - origin (str): Origin class / logger (e.g. com.mirth.connect.server.Mirth) or None
            - message (str): Main message text (from the first line)
            - cause (str): Root cause message extracted from "Caused by: ..." lines, or None
            - channel_name (str): Channel name extracted from thread context, or None
            - channel_id (str): Channel ID extracted from thread context, or None
            - connector_name (str): Connector/Destination name extracted from thread context, or None
            - raw_entry (str): Original unmodified entry string
    """
    result = {
        'type': None,
        'datetime': None,
        'thread': None,
        'origin': None,
        'message': None,
        'cause': None,
        'channel_name': None,
        'channel_id': None,
        'connector_name': None,
        'raw_entry': log_entry_str
    }
    
    if not log_entry_str:
        return result
        
    lines = log_entry_str.splitlines()
    if not lines:
        return result
        
    first_line = lines[0]
    
    # Detailed regex to parse the first line of a Mirth log entry
    # Example: ERROR 2026-05-06 15:35:57.200 [Main Server Thread] com.mirth.connect.server.Mirth: Error message...
    pattern = re.compile(
        r'^([A-Z]+)\s+'                                           # 1. Log Level (Type)
        r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d{3})?)\s+'  # 2. Datetime
        r'\[(.*?)\]\s+'                                           # 3. Thread
        r'(.*?):\s*'                                              # 4. Class / Origin
        r'(.*)$'                                                  # 5. Message
    )
    
    match = pattern.match(first_line)
    if match:
        result['type'] = match.group(1).strip()
        result['datetime'] = match.group(2).strip()
        result['thread'] = match.group(3).strip()
        result['origin'] = match.group(4).strip()
        result['message'] = match.group(5).strip()
        
        # Try to parse Mirth specific channel information from the thread name
        # Thread pattern: "... Task on CHANNEL_NAME (CHANNEL_ID), CONNECTOR_NAME (CONNECTOR_INDEX) < ..."
        thread_name = result['thread']
        channel_match = re.search(
            r'\bon\s+([^(,]+)\s*\(([^)]+)\)(?:,\s*([^(,]+)\s*(?:\((\d+)\))?)?',
            thread_name
        )
        if channel_match:
            result['channel_name'] = channel_match.group(1).strip()
            result['channel_id'] = channel_match.group(2).strip()
            if channel_match.group(3):
                result['connector_name'] = channel_match.group(3).strip()
    else:
        # Fallback if first line does not match standard log format
        result['message'] = first_line.strip()
        
    # Search for stack trace and root causes in subsequent lines
    # The last "Caused by:" in the chain is usually the most specific/root cause
    for line in lines[1:]:
        stripped = line.strip()
        if stripped.startswith("Caused by:"):
            result['cause'] = stripped[len("Caused by:"):].strip()
            
    return result

def mirth_log_filter(log_entries, search_terms=None):
    """
    Filters a list of log entries. 
    Keeps only entries that contain all of the specified search terms (case-insensitive).
    
    Args:
        log_entries (list): List of log entries. Can be a list of raw entry strings or decoded dictionaries.
        search_terms (list[str]): List of search terms. If None or empty, returns original list.
        
    Returns:
        list: Filtered list of log entries (maintains original format - strings or dicts).
    """
    if not search_terms:
        return log_entries
        
    filtered = []
    for entry in log_entries:
        # Get textual content of the entry
        if isinstance(entry, dict):
            text_content = entry.get('raw_entry', '')
            if not text_content:
                # Fallback to joining dictionary values
                text_content = " ".join(str(v) for v in entry.values())
        else:
            text_content = str(entry)
            
        # Match all terms (case-insensitive)
        matches_all = True
        for term in search_terms:
            if term.lower() not in text_content.lower():
                matches_all = False
                break
                
        if matches_all:
            filtered.append(entry)
            
    return filtered


if __name__ == '__main__':
    import sys
    import argparse
    import datetime
    from collections import Counter
    from tabulate import tabulate
    
    def safe_print(text=""):
        """
        Safely prints text to stdout, handling potential UnicodeEncodeError on Windows terminals.
        """
        try:
            print(text)
        except UnicodeEncodeError:
            encoding = sys.stdout.encoding or 'utf-8'
            try:
                print(text.encode(encoding, errors='replace').decode(encoding))
            except Exception:
                print(text.encode('ascii', errors='replace').decode('ascii'))

    def print_table(data, headers, tablefmt="fancy_grid"):
        """
        Prints a table using tabulate, falling back to ASCII-only 'grid' or 'simple' format
        if the terminal does not support Unicode characters.
        """
        try:
            table_str = tabulate(data, headers=headers, tablefmt=tablefmt)
            print(table_str)
        except UnicodeEncodeError:
            try:
                # Fallback to standard ASCII grid
                table_str = tabulate(data, headers=headers, tablefmt="grid")
                safe_print(table_str)
            except Exception:
                # Fallback to simple formatting
                table_str = tabulate(data, headers=headers, tablefmt="simple")
                safe_print(table_str)

    def display_statistics(filename):
        """
        Reads a Mirth log file, parses and decodes its content, computes various 
        statistics, and displays them in beautifully formatted tables.
        
        Args:
            filename (str): Path to the log file.
        """
        try:
            raw_entries = mirth_file_parser(filename)
        except Exception as e:
            print(f"Error parsing log file: {e}", file=sys.stderr)
            sys.exit(1)
            
        decoded_entries = [mirth_log_decoder(entry) for entry in raw_entries]
        
        # 1. General Stats
        file_size_kb = os.path.getsize(filename) / 1024
        
        # Count total lines
        try:
            with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
                total_lines = len(f.readlines())
        except Exception:
            total_lines = 0
            
        # File modification time
        try:
            mtime = os.path.getmtime(filename)
            mtime_str = datetime.datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            mtime_str = "Unknown"
            
        # Oldest and newest message timestamps
        timestamps = [e['datetime'] for e in decoded_entries if e['datetime']]
        oldest_msg = min(timestamps) if timestamps else "N/A"
        newest_msg = max(timestamps) if timestamps else "N/A"
        
        general_stats = [
            ["Log File", os.path.abspath(filename)],
            ["File Size", f"{file_size_kb:.2f} KB"],
            ["File Modification Date", mtime_str],
            ["Total Raw Lines", total_lines],
            ["Parsed Log Entries", len(decoded_entries)],
            ["Oldest Message Timestamp", oldest_msg],
            ["Newest Message Timestamp", newest_msg]
        ]
        
        # 2. Log Levels Distribution (Global)
        level_counts = Counter(e['type'] for e in decoded_entries if e['type'] is not None)
        unknown_count = sum(1 for e in decoded_entries if e['type'] is None)
        if unknown_count > 0:
            level_counts['UNKNOWN/OTHER'] = unknown_count
            
        total_parsed = len(decoded_entries)
        level_stats = []
        for level, count in level_counts.most_common():
            percentage = (count / total_parsed * 100) if total_parsed > 0 else 0
            level_stats.append([level, count, f"{percentage:.1f}%"])
            
        # 3. Channels Distribution & Detailed Stats
        # Group by channel name (or "Global / Server" if None)
        channel_groups = {}
        for e in decoded_entries:
            ch_name = e['channel_name'] if e['channel_name'] else "Global / Server"
            if ch_name not in channel_groups:
                channel_groups[ch_name] = []
            channel_groups[ch_name].append(e)
            
        channel_summary_stats = []
        for ch_name, entries in channel_groups.items():
            # Get channel ID from the first entry that has it, otherwise N/A
            ch_ids = [e['channel_id'] for e in entries if e['channel_id']]
            ch_id = ch_ids[0] if ch_ids else "N/A"
            
            total_ch_logs = len(entries)
            
            # Level counts for this channel
            ch_levels = Counter(e['type'] for e in entries if e['type'] is not None)
            info_cnt = ch_levels.get('INFO', 0)
            error_cnt = ch_levels.get('ERROR', 0)
            other_cnt = total_ch_logs - info_cnt - error_cnt
            
            # Last exchange (most recent timestamp)
            ch_dates = [e['datetime'] for e in entries if e['datetime']]
            last_exchange = max(ch_dates) if ch_dates else "N/A"
            
            channel_summary_stats.append([
                ch_name,
                ch_id,
                info_cnt,
                error_cnt,
                other_cnt,
                total_ch_logs,
                last_exchange
            ])
            
        # Sort by total logs count descending, keep "Global / Server" at the top if present
        channel_summary_stats.sort(key=lambda x: (x[0] != "Global / Server", -x[5]))
        
        # 4. Top Origins
        origin_groups = {}
        for e in decoded_entries:
            orig = e['origin']
            if orig is not None:
                if orig not in origin_groups:
                    origin_groups[orig] = []
                origin_groups[orig].append(e)
        
        origin_stats = []
        for orig, entries in origin_groups.items():
            count = len(entries)
            orig_dates = [e['datetime'] for e in entries if e['datetime']]
            last_ex = max(orig_dates) if orig_dates else "N/A"
            origin_stats.append([orig, count, last_ex])
        origin_stats.sort(key=lambda x: x[1], reverse=True)
        origin_stats = origin_stats[:10]
        
        # 5. Top Threads
        thread_groups = {}
        for e in decoded_entries:
            thr = e['thread']
            if thr is not None:
                if thr not in thread_groups:
                    thread_groups[thr] = []
                thread_groups[thr].append(e)
                
        thread_stats = []
        for thr, entries in thread_groups.items():
            count = len(entries)
            thr_dates = [e['datetime'] for e in entries if e['datetime']]
            last_ex = max(thr_dates) if thr_dates else "N/A"
            thread_stats.append([thr, count, last_ex])
        thread_stats.sort(key=lambda x: x[1], reverse=True)
        thread_stats = thread_stats[:10]
        
        # 6. Errors & Causes
        errors = [e for e in decoded_entries if e['type'] == 'ERROR']
        error_stats = []
        if errors:
            error_groups = {}
            for e in errors:
                detail = f"Cause: {e['cause']}" if e['cause'] else f"Message: {e['message']}"
                channel = e['channel_name'] if e['channel_name'] else "Global / Server"
                key = (detail, channel)
                if key not in error_groups:
                    error_groups[key] = []
                error_groups[key].append(e)
                
            for (detail, channel), group_entries in error_groups.items():
                count = len(group_entries)
                dates = [e['datetime'] for e in group_entries if e['datetime']]
                first_occ = min(dates) if dates else "N/A"
                last_occ = max(dates) if dates else "N/A"
                
                # If first and last occurrence are the same, show "." for last occurrence
                display_last_occ = "." if first_occ == last_occ else last_occ
                
                # Truncate detail for display
                display_detail = detail
                if len(display_detail) > 60:
                    display_detail = display_detail[:57] + "..."
                    
                error_stats.append([channel, display_detail, count, first_occ, display_last_occ])
                
            # Sort by count descending, then by channel
            error_stats.sort(key=lambda x: (-x[2], x[0]))

        # Print Report
        safe_print("\n" + "=" * 80)
        safe_print(" MIRTH LOG ANALYSIS REPORT ".center(80, "="))
        safe_print("=" * 80 + "\n")
        
        safe_print("### 1. GENERAL STATISTICS")
        print_table(general_stats, headers=["Metric", "Value"], tablefmt="fancy_grid")
        safe_print("\n")
        
        safe_print("### 2. LOG LEVELS DISTRIBUTION (GLOBAL)")
        print_table(level_stats, headers=["Level", "Count", "Percentage"], tablefmt="fancy_grid")
        safe_print("\n")
        
        safe_print("### 3. STATISTICS BY CHANNEL")
        print_table(
            channel_summary_stats, 
            headers=["Channel Name", "Channel ID", "INFO", "ERROR", "OTHER", "Total Logs", "Last Exchange"], 
            tablefmt="fancy_grid"
        )
        safe_print("\n")
        
        safe_print("### 4. TOP LOGGING ORIGINS (CLASSES)")
        print_table(origin_stats, headers=["Origin Class / Logger", "Count", "Last Exchange"], tablefmt="fancy_grid")
        safe_print("\n")
        
        safe_print("### 5. TOP LOGGING THREADS")
        print_table(thread_stats, headers=["Thread Name", "Count", "Last Exchange"], tablefmt="fancy_grid")
        safe_print("\n")
        
        if error_stats:
            safe_print("### 6. DETECTED ERRORS & ROOT CAUSES")
            print_table(
                error_stats, 
                headers=["Channel Name", "Error Details / Cause", "Count", "First Occurrence", "Last Occurrence"], 
                tablefmt="fancy_grid"
            )
            safe_print("\n")

    def main():
        parser = argparse.ArgumentParser(description="Parse Mirth log files and display statistics.")
        parser.add_argument(
            "logfile", 
            nargs="?", 
            default=r"Ressources\mirth-exemple.log",
            help=r"Path to the Mirth log file (default: Ressources\mirth-exemple.log)"
        )
        args = parser.parse_args()
        
        if not os.path.exists(args.logfile):
            safe_print(f"Error: Log file '{args.logfile}' does not exist.")
            sys.exit(1)
            
        display_statistics(args.logfile)

    main()

