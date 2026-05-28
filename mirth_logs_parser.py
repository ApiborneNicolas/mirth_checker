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


def mirth_file_parser(filename, date: int = 1, trait_rotatelog: bool = False):
    """
    Parses Mirth log files and groups lines belonging to the same log entry.
    Returns details and statistics for each file parsed.
    
    Args:
        filename (str): Path to the log file.
        date (int): Date offset: 1 = all content, 0 = today, -1 = J-1, -x = J-X.
        trait_rotatelog (bool): If True, extend parsing to rotated files ({filename}.x)
                                that meet the date criteria.
        
    Returns:
        list[dict]: A list of dicts, one for each file read, containing:
            - filename (str): Path of the file
            - filestat (dict): File statistics (size_bytes, mtime, ctime)
            - raw_lines (int): Total number of lines in the file
            - parsed_logs (int): Number of log entries matching the date criteria
            - parsed_lines (list[str]): The parsed log entry strings
    """
    import datetime
    
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Log file not found: {filename}")
        
    # Determine the target date and filter criteria if offset is <= 0
    target_date = None
    target_date_str = None
    if date <= 0:
        target_date = datetime.date.today() + datetime.timedelta(days=date)
        target_date_str = target_date.strftime("%Y-%m-%d")
        
    # Get all candidate files to parse
    files_to_check = [filename]
    if trait_rotatelog:
        dir_name = os.path.dirname(os.path.abspath(filename))
        base_name = os.path.basename(filename)
        if os.path.exists(dir_name):
            for f in os.listdir(dir_name):
                # Match logrotate files, e.g. mirth.log.1, mirth.log.2
                if re.match(rf"^{re.escape(base_name)}\.\d+$", f):
                    files_to_check.append(os.path.join(dir_name, f))
                    
    # Filter files using modification date (mtime)
    valid_files = []
    for filepath in files_to_check:
        if not os.path.exists(filepath):
            continue
        if date <= 0 and target_date is not None:
            try:
                mtime = os.path.getmtime(filepath)
                mtime_date = datetime.date.fromtimestamp(mtime)
                # Skip files modified before the target date
                if mtime_date < target_date:
                    continue
            except Exception:
                pass
        valid_files.append(filepath)
        
    # Sort files by modification date ascending so they are parsed in chronological order
    try:
        valid_files.sort(key=lambda x: os.path.getmtime(x))
    except Exception:
        pass
        
    results = []
    # Pattern matching log entry start: e.g. ERROR 2026-05-06 15:35:57.200 or INFO  2026-05-06 15:36:07.275
    # Standard format: LEVEL YYYY-MM-DD HH:MM:SS.SSS
    header_pattern = re.compile(r'^[A-Z]+\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}')
    date_extractor = re.compile(r'^[A-Z]+\s+(\d{4}-\d{2}-\d{2})')
    
    for filepath in valid_files:
        # Get file stats
        try:
            stat_info = os.stat(filepath)
            mtime_str = datetime.datetime.fromtimestamp(stat_info.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            ctime_str = datetime.datetime.fromtimestamp(stat_info.st_ctime).strftime('%Y-%m-%d %H:%M:%S')
            filestat = {
                "size_bytes": stat_info.st_size,
                "mtime": mtime_str,
                "ctime": ctime_str
            }
        except Exception:
            filestat = {
                "size_bytes": 0,
                "mtime": "Unknown",
                "ctime": "Unknown"
            }

        lines = []
        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
        except Exception:
            try:
                with open(filepath, 'r', encoding='latin-1', errors='replace') as f:
                    lines = f.readlines()
            except Exception:
                pass
                
        file_entries = []
        current_entry = []
        for line in lines:
            if header_pattern.match(line):
                if current_entry:
                    should_append = True
                    if date <= 0 and target_date_str is not None:
                        m = date_extractor.match(current_entry[0])
                        if not m or m.group(1) != target_date_str:
                            should_append = False
                    if should_append:
                        file_entries.append("".join(current_entry))
                    current_entry = []
                current_entry.append(line)
            else:
                if current_entry:
                    current_entry.append(line)
                else:
                    # If file starts with lines that have no header, group them as the first entry
                    current_entry.append(line)
                    
        if current_entry:
            should_append = True
            if date <= 0 and target_date_str is not None:
                m = date_extractor.match(current_entry[0])
                if not m or m.group(1) != target_date_str:
                    should_append = False
            if should_append:
                file_entries.append("".join(current_entry))
                
        results.append({
            "filename": filepath,
            "filestat": filestat,
            "raw_lines": len(lines),
            "parsed_logs": len(file_entries),
            "parsed_lines": file_entries
        })
        
    return results


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

    def display_statistics(filename, date=1, trait_rotatelog=False):
        """
        Reads Mirth log file(s), parses and decodes their content, computes various 
        statistics, and displays them in beautifully formatted tables.
        
        Args:
            filename (str): Path to the log file.
            date (int): Date offset: 1 = all content, 0 = today, -1 = J-1, -x = J-X.
            trait_rotatelog (bool): If True, extend parsing to rotated files.
        """
        def format_to_french_date(dt_str):
            if not dt_str or dt_str in ("N/A", "Unknown", "Inconnu"):
                return dt_str
            try:
                # Handle YYYY-MM-DD HH:MM:SS.SSS or YYYY-MM-DD HH:MM:SS
                parts = dt_str.split('.')
                base_dt = datetime.datetime.strptime(parts[0], '%Y-%m-%d %H:%M:%S')
                french_base = base_dt.strftime('%d/%m/%Y %H:%M:%S')
                if len(parts) > 1:
                    return f"{french_base}.{parts[1]}"
                return french_base
            except Exception:
                return dt_str

        try:
            parsed_files = mirth_file_parser(filename, date=date, trait_rotatelog=trait_rotatelog)
        except Exception as e:
            print(f"Error parsing log file: {e}", file=sys.stderr)
            sys.exit(1)
            
        # Extract and decode all log entries
        decoded_entries = []
        for file_info in parsed_files:
            for entry in file_info['parsed_lines']:
                decoded_entries.append(mirth_log_decoder(entry))
                
        # 1. Analyzed Files Details
        files_details_data = []
        total_size_bytes = 0
        total_raw_lines = 0
        total_parsed_logs = 0
        
        for file_info in parsed_files:
            stats = file_info['filestat']
            size_kb = stats['size_bytes'] / 1024
            total_size_bytes += stats['size_bytes']
            total_raw_lines += file_info['raw_lines']
            total_parsed_logs += file_info['parsed_logs']
            
            files_details_data.append([
                os.path.basename(file_info['filename']),
                f"{size_kb:.2f} KB",
                format_to_french_date(stats['ctime']),
                format_to_french_date(stats['mtime']),
                file_info['raw_lines'],
                file_info['parsed_logs']
            ])
            
        # Oldest and newest message timestamps
        timestamps = [e['datetime'] for e in decoded_entries if e['datetime']]
        oldest_msg = format_to_french_date(min(timestamps)) if timestamps else "N/A"
        newest_msg = format_to_french_date(max(timestamps)) if timestamps else "N/A"
        
        # Main File modification time
        try:
            mtime = os.path.getmtime(filename)
            mtime_str = datetime.datetime.fromtimestamp(mtime).strftime('%d/%m/%Y %H:%M:%S')
        except Exception:
            mtime_str = "Inconnu"
            
        general_stats = [
            ["Fichier de log principal", os.path.abspath(filename)],
            ["Total des fichiers scannés", len(parsed_files)],
            ["Taille totale (cumulée)", f"{total_size_bytes / 1024:.2f} KB"],
            ["Date de modification du fichier principal", mtime_str],
            ["Total des lignes brutes", total_raw_lines],
            ["Total des entrées de log analysées", total_parsed_logs],
            ["Date/heure du message le plus ancien", oldest_msg],
            ["Date/heure du message le plus récent", newest_msg]
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
            ch_name_display = "Global / Serveur" if ch_name == "Global / Server" else ch_name
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
            last_exchange = format_to_french_date(max(ch_dates)) if ch_dates else "N/A"
            
            channel_summary_stats.append([
                ch_name_display,
                ch_id,
                info_cnt,
                error_cnt,
                other_cnt,
                total_ch_logs,
                last_exchange
            ])
            
        # Sort by total logs count descending, keep "Global / Serveur" at the top if present
        channel_summary_stats.sort(key=lambda x: (x[0] != "Global / Serveur", -x[5]))
        
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
            last_ex = format_to_french_date(max(orig_dates)) if orig_dates else "N/A"
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
            last_ex = format_to_french_date(max(thr_dates)) if thr_dates else "N/A"
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
                detail_display = detail.replace("Cause:", "Cause :").replace("Message:", "Message :")
                channel = e['channel_name'] if e['channel_name'] else "Global / Server"
                key = (detail_display, channel)
                if key not in error_groups:
                    error_groups[key] = []
                error_groups[key].append(e)
                
            for (detail_display, channel), group_entries in error_groups.items():
                channel_display = "Global / Serveur" if channel == "Global / Server" else channel
                count = len(group_entries)
                dates = [e['datetime'] for e in group_entries if e['datetime']]
                first_occ = format_to_french_date(min(dates)) if dates else "N/A"
                last_occ = format_to_french_date(max(dates)) if dates else "N/A"
                
                # If first and last occurrence are the same, show "." for last occurrence
                display_last_occ = "." if first_occ == last_occ else last_occ
                
                # Truncate detail for display
                display_detail = detail_display
                if len(display_detail) > 60:
                    display_detail = display_detail[:57] + "..."
                    
                error_stats.append([channel_display, display_detail, count, first_occ, display_last_occ])
                
            # Sort by count descending, then by channel
            error_stats.sort(key=lambda x: (-x[2], x[0]))

        # Print Report
        safe_print("\n" + "=" * 80)
        safe_print(" RAPPORT D'ANALYSE DES LOGS MIRTH ".center(80, "="))
        safe_print("=" * 80 + "\n")
        
        safe_print("### 1. STATISTIQUES DÉTAILLÉES DES FICHIERS")
        print_table(
            files_details_data, 
            headers=["Nom du fichier", "Taille", "Date de création", "Date de modification", "Lignes brutes", "Logs analysés"], 
            tablefmt="fancy_grid"
        )
        safe_print("\n")
        
        safe_print("### 2. STATISTIQUES GÉNÉRALES")
        print_table(general_stats, headers=["Indicateur", "Valeur"], tablefmt="fancy_grid")
        safe_print("\n")
        
        if decoded_entries:
            safe_print("### 3. RÉPARTITION DES NIVEAUX DE LOGS (GLOBAL)")
            print_table(level_stats, headers=["Niveau", "Nombre", "Pourcentage"], tablefmt="fancy_grid")
            safe_print("\n")
            
            safe_print("### 4. STATISTIQUES PAR CANAL (CHANNEL)")
            print_table(
                channel_summary_stats, 
                headers=["Nom du canal", "ID du canal", "INFO", "ERROR", "AUTRE", "Total des logs", "Dernier échange"], 
                tablefmt="fancy_grid"
            )
            safe_print("\n")
            
            safe_print("### 5. PRINCIPALES ORIGINES DES LOGS (CLASSES)")
            print_table(origin_stats, headers=["Classe d'origine / Logger", "Nombre", "Dernier échange"], tablefmt="fancy_grid")
            safe_print("\n")
            
            safe_print("### 6. PRINCIPAUX THREADS DE LOGS")
            print_table(thread_stats, headers=["Nom du thread", "Nombre", "Dernier échange"], tablefmt="fancy_grid")
            safe_print("\n")
            
            if error_stats:
                safe_print("### 7. ERREURS DÉTECTÉES & CAUSES RACINES")
                print_table(
                    error_stats, 
                    headers=["Nom du canal", "Détails de l'erreur / Cause", "Nombre", "Première occurrence", "Dernière occurrence"], 
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
        parser.add_argument(
            "-d", "--date",
            type=int,
            default=1,
            help="Date offset: 1 = all content, 0 = today, -1 = J-1, -x = J-X"
        )
        parser.add_argument(
            "-r", "--trait-rotatelog",
            action="store_true",
            help="Extend parsing to logrotate files ({filename}.x)"
        )
        parser.add_argument(
            "-m", "--mail",
            type=str,
            default=None,
            help="Email address to send the report to"
        )
        args = parser.parse_args()
        
        if not os.path.exists(args.logfile):
            safe_print(f"Error: Log file '{args.logfile}' does not exist.")
            sys.exit(1)
            
        import io
        import contextlib
        
        report_buffer = io.StringIO()
        with contextlib.redirect_stdout(report_buffer):
            display_statistics(args.logfile, date=args.date, trait_rotatelog=args.trait_rotatelog)
        report_content = report_buffer.getvalue()
        
        # Display report on terminal
        safe_print(report_content)
        
        if args.mail:
            try:
                from quickmail import sendmail
                import html
                subject = f"Rapport d'analyse de log Mirth - {os.path.basename(args.logfile)}"
                
                # Friendly intro text to reduce spam score
                intro_text = (
                    "Bonjour,\n\n"
                    "Veuillez trouver ci-dessous le rapport d'analyse automatique pour le fichier de log Mirth.\n\n"
                    "--------------------------------------------------------------------------------\n\n"
                )
                plain_message = intro_text + report_content
                
                # Helper to convert plain-text report to beautiful structured HTML
                def format_report_to_html(report_text):
                    lines = report_text.splitlines()
                    html_parts = []
                    current_table_lines = []
                    
                    def flush_table():
                        if current_table_lines:
                            table_text = "\n".join(current_table_lines)
                            escaped = html.escape(table_text)
                            html_parts.append(f'<div class="table-container"><pre>{escaped}</pre></div>')
                            current_table_lines.clear()

                    for line in lines:
                        stripped = line.strip()
                        # Skip banner separators (lots of =)
                        if len(stripped) >= 10 and all(c == '=' for c in stripped):
                            continue
                            
                        # Check for main title
                        if "RAPPORT D'ANALYSE DES LOGS MIRTH" in stripped:
                            flush_table()
                            title_text = stripped.strip("= ")
                            html_parts.append(f'<h2 style="text-align: center; color: #2c3e50; margin-bottom: 25px; font-size: 20px; border-bottom: 2px solid #34495e; padding-bottom: 10px;">{html.escape(title_text)}</h2>')
                            continue
                            
                        if stripped.startswith("### "):
                            flush_table()
                            header_text = stripped[4:]
                            html_parts.append(f'<h3>{html.escape(header_text)}</h3>')
                        else:
                            is_table_line = False
                            if stripped:
                                first_char = stripped[0]
                                # Match standard table borders (+, |, ?, ?) and box drawing chars U+2500 to U+257F
                                if first_char in ('+', '|', '?', '?') or ('\u2500' <= first_char <= '\u257f'):
                                    is_table_line = True
                                    
                            if is_table_line:
                                current_table_lines.append(line)
                            else:
                                if stripped:
                                    flush_table()
                                    html_parts.append(f'<p style="font-size: 14px; color: #495057; margin: 10px 0;">{html.escape(line)}</p>')
                                else:
                                    if current_table_lines:
                                        current_table_lines.append(line)
                                        
                    flush_table()
                    return "\n".join(html_parts)
                
                formatted_html_report = format_report_to_html(report_content)
                
                html_message = f"""
                <html>
                <head>
                    <style>
                        body {{
                            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                            background-color: #f8f9fa;
                            color: #212529;
                            padding: 20px;
                            margin: 0;
                        }}
                        .container {{
                            width: 98%;
                            max-width: 100%;
                            background: #ffffff;
                            border: 1px solid #e9ecef;
                            border-radius: 6px;
                            padding: 25px;
                            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
                            margin: 0 auto;
                            box-sizing: border-box;
                        }}
                        h2 {{
                            color: #2c3e50;
                            border-bottom: 2px solid #dee2e6;
                            padding-bottom: 10px;
                            margin-top: 0;
                        }}
                        h3 {{
                            color: #2c3e50;
                            font-size: 16px;
                            border-left: 4px solid #3498db;
                            padding-left: 10px;
                            margin-top: 30px;
                            margin-bottom: 15px;
                        }}
                        .table-container {{
                            width: 100%;
                            overflow-x: auto;
                            border: 1px solid #dee2e6;
                            border-radius: 4px;
                            background-color: #f1f3f5;
                            margin: 15px 0;
                        }}
                        .table-container pre {{
                            margin: 0;
                            padding: 15px;
                            font-family: "Consolas", "Courier New", Courier, monospace;
                            font-size: 13px;
                            line-height: 1.5;
                            white-space: pre;
                            word-wrap: normal;
                            display: inline-block;
                            min-width: 100%;
                            box-sizing: border-box;
                        }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h2>Rapport d'analyse de log Mirth</h2>
                        <p style="font-size: 14px; color: #495057;">Bonjour,</p>
                        <p style="font-size: 14px; color: #495057;">Veuillez trouver ci-dessous le rapport d'analyse automatique pour le fichier de log Mirth.</p>
                        <p style="font-size: 12px; color: #6c757d;">G&eacute;n&eacute;r&eacute; le {datetime.datetime.now().strftime('%Y-%m-%d à %H:%M:%S')}</p>
                        <hr style="border: 0; border-top: 1px solid #dee2e6; margin: 20px 0;">
                        {formatted_html_report}
                    </div>
                </body>
                </html>
                """
                # Send email with both alternatives and plain text report as attachment
                success = sendmail(
                    sujet=subject,
                    message=plain_message,
                    dest=args.mail,
                    html=html_message,
                    attachment_name="rapport.txt",
                    attachment_content=report_content
                )
                if success:
                    safe_print(f"Report successfully sent to {args.mail}")
                else:
                    safe_print(f"Failed to send report to {args.mail}")
            except ImportError:
                safe_print("Error: quickmail.py not found or failed to import sendmail.")
            except Exception as e:
                safe_print(f"Error sending email: {e}")

    main()

