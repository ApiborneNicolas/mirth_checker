import psutil
import platform
import datetime
import socket
from ping3 import ping
from tabulate import tabulate

# ==============================================================================
# PARTIE 1 : LA LIBRAIRIE (FONCTIONS DÉDIÉES)
# ==============================================================================

def get_now_datetime() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_hostname() -> str:
    return platform.node() or socket.gethostname()

def get_boot_time() -> str:
    return datetime.datetime.fromtimestamp(psutil.boot_time()).strftime("%Y-%m-%d %H:%M:%S")

def get_os_name() -> str:
    return platform.system()

def get_os_version() -> str:
    return platform.version()

def get_cpu_count_logical() -> int:
    return psutil.cpu_count()

def get_cpu_count_physical() -> int:
    return psutil.cpu_count(logical=False)



def get_cpu_usage_global(delay: float = 0.1) -> float:
    return psutil.cpu_percent(interval=delay)

def get_process_list(delay: float = 0.1) -> list:
    """Retourne la liste style gestionnaire de tâches avec consommation CPU réelle et globalisée"""
    active_procs = []
    for proc in psutil.process_iter(['pid', 'name', 'username', 'memory_percent', 'memory_info']):
        try:
            proc.cpu_percent(interval=None)
            active_procs.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    if not active_procs:
        return []

    import time
    time.sleep(delay)

    processes = []
    num_cores = psutil.cpu_count() or 1
    for proc in active_procs:
        try:
            cpu = proc.cpu_percent(interval=None)
            global_cpu = cpu / num_cores
            mem_info = proc.info['memory_info']
            rss_mb = round(mem_info.rss / (1024**2), 2) if mem_info else 0.0
            info = {
                'pid': proc.info['pid'],
                'name': proc.info['name'],
                'username': proc.info['username'],
                'cpu_percent': global_cpu,
                'memory_rss_mb': rss_mb,
                'memory_percent': proc.info['memory_percent']
            }
            processes.append(info)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return processes

def get_cpu(cible: str = "all", delay: float = 0.1) -> dict:
    if cible.upper() == "ALL":
        return {
            "usage_global": get_cpu_usage_global(delay),
            "coeurs_phys": get_cpu_count_physical(),
            "coeurs_logiq": get_cpu_count_logical()
        }
    elif cible.upper() == "LISTALL":
        return {"processes": get_process_list(delay)}
    else:
        # Recherche et agrégation de tous les processus correspondant à la cible
        matched_procs = []
        for proc in psutil.process_iter(['name']):
            try:
                name = proc.info['name']
                if name and cible.lower() in name.lower():
                    proc.cpu_percent(interval=None)
                    matched_procs.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        if not matched_procs:
            return {"error": "Processus non trouvé"}

        import time
        time.sleep(delay)

        total_cpu = 0.0
        num_cores = psutil.cpu_count() or 1
        for proc in matched_procs:
            try:
                total_cpu += proc.cpu_percent(interval=None)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        global_cpu = round(total_cpu / num_cores, 2)
        return {"name": cible, "usage": global_cpu, "instances": len(matched_procs)}

def get_mem_total() -> float:
    return round(psutil.virtual_memory().total / (1024**3), 2)

def get_mem_available() -> float:
    return round(psutil.virtual_memory().available / (1024**3), 2)

def get_mem_percent() -> float:
    return psutil.virtual_memory().percent

def get_mem(cible: str = "all") -> dict:
    if cible.upper() == "ALL":
        return {
            "total_gb": get_mem_total(),
            "available_gb": get_mem_available(),
            "percent": get_mem_percent()
        }
    elif cible.upper() == "LISTALL":
        # Tri par consommation mémoire pour LISTALL
        procs = sorted(get_process_list(), key=lambda x: x['memory_percent'], reverse=True)
        return {"processes": procs}
    else:
        for proc in psutil.process_iter(['name', 'memory_info']):
            if cible.lower() in proc.info['name'].lower():
                return {
                    "name": proc.info['name'], 
                    "rss_mb": round(proc.info['memory_info'].rss / (1024**2), 2)
                }
    return {"error": "Processus non trouvé"}

def get_storage_partitions() -> list:
    return psutil.disk_partitions()

def get_disk_usage(path: str) -> dict:
    usage = psutil.disk_usage(path)
    return {
        "path": path,
        "total": round(usage.total / (1024**3), 2),
        "used": round(usage.used / (1024**3), 2),
        "free": round(usage.free / (1024**3), 2),
        "percent": usage.percent
    }

def get_network_io() -> dict:
    io = psutil.net_io_counters()
    return {"sent_mb": round(io.bytes_sent / (1024**2), 2), "recv_mb": round(io.bytes_recv / (1024**2), 2)}

def get_tcp_udp_count() -> dict:
    conns = psutil.net_connections()
    return {
        "tcp": len([c for c in conns if c.type == socket.SOCK_STREAM]),
        "udp": len([c for c in conns if c.type == socket.SOCK_DGRAM])
    }

def get_active_connections(cible: str = "ALL") -> list:
    """Retourne la liste des connexions TCP actives (ESTABLISHED) avec leurs adresses locale et distante"""
    results = []
    filter_targets = []
    if cible.upper() != "ALL":
        filter_targets = [t.strip().lower() for t in cible.split(",") if t.strip()]

    try:
        conns = psutil.net_connections(kind='tcp')
    except (psutil.AccessDenied, Exception):
        return []

    pid_names = {}
    for conn in conns:
        if conn.status == 'ESTABLISHED':
            pid = conn.pid
            proc_name = "-"
            if pid:
                if pid not in pid_names:
                    try:
                        proc = psutil.Process(pid)
                        pid_names[pid] = proc.name()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pid_names[pid] = "Inconnu"
                proc_name = pid_names[pid]

            if filter_targets:
                if not any(t in proc_name.lower() for t in filter_targets):
                    continue

            laddr_str = f"{conn.laddr.ip}:{conn.laddr.port}"
            raddr_str = f"{conn.raddr.ip}:{conn.raddr.port}" if conn.raddr else "-"
            results.append({
                "laddr": laddr_str,
                "raddr": raddr_str,
                "pid": pid if pid else "-",
                "proc_name": proc_name
            })
    return results

def get_windows_adapter_descriptions() -> dict:
    """Retourne un dictionnaire associant les noms conviviaux des adaptateurs réseau à leur description de pilote sur Windows"""
    import winreg
    descriptions = {}
    net_cfg_key = r"SYSTEM\CurrentControlSet\Control\Network\{4D36E972-E325-11CE-BFC1-08002BE10318}"
    class_key = r"SYSTEM\CurrentControlSet\Control\Class\{4D36E972-E325-11CE-BFC1-08002BE10318}"

    guid_to_desc = {}
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, class_key) as key:
            i = 0
            while True:
                try:
                    subkey_name = winreg.EnumKey(key, i)
                    with winreg.OpenKey(key, subkey_name) as subkey:
                        try:
                            driver_desc, _ = winreg.QueryValueEx(subkey, "DriverDesc")
                            net_cfg_id, _ = winreg.QueryValueEx(subkey, "NetCfgInstanceId")
                            guid_to_desc[net_cfg_id] = driver_desc
                        except OSError:
                            pass
                    i += 1
                except OSError:
                    break
    except Exception:
        pass

    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, net_cfg_key) as key:
            i = 0
            while True:
                try:
                    guid = winreg.EnumKey(key, i)
                    conn_path = fr"{guid}\Connection"
                    try:
                        with winreg.OpenKey(key, conn_path) as conn_key:
                            name, _ = winreg.QueryValueEx(conn_key, "Name")
                            if guid in guid_to_desc:
                                descriptions[name] = guid_to_desc[guid]
                    except OSError:
                        pass
                    i += 1
                except OSError:
                    break
    except Exception:
        pass

    return descriptions

def get_vpn_status() -> list:
    """Retourne uniquement les noms des interfaces VPN actives"""
    active_vpns = []
    descriptions = {}
    if platform.system() == 'Windows':
        descriptions = get_windows_adapter_descriptions()

    for name, s in psutil.net_if_stats().items():
        if s.isup:
            desc = descriptions.get(name, "")
            is_vpn = any(x in name.lower() or x in desc.lower() for x in ["vpn", "tap", "tun", "ppp", "forti", "openvpn", "wireguard", "cisco", "tailscale"])
            if is_vpn:
                active_vpns.append(name)
    return active_vpns

def get_vpn_interfaces() -> list:
    """Retourne la liste de toutes les interfaces réseau détectées (non désactivées) avec leurs stats d'E/S et adresses IP"""
    results = []
    stats = psutil.net_if_stats()
    addrs = psutil.net_if_addrs()

    io_counters = {}
    try:
        io_counters = psutil.net_io_counters(pernic=True)
    except:
        pass

    descriptions = {}
    if platform.system() == 'Windows':
        descriptions = get_windows_adapter_descriptions()

    for name, stat in stats.items():
        # Ignorer le loopback
        if "loopback" in name.lower() or name.lower() == "lo":
            continue

        desc = descriptions.get(name, "")
        ip_addrs = []
        if name in addrs:
            for addr in addrs[name]:
                if addr.family == socket.AF_INET:
                    ip_addrs.append(addr.address)
                elif addr.family == socket.AF_INET6:
                    # Exclure les adresses de lien local fe80 pour la clarté
                    if not addr.address.startswith("fe80"):
                        ip_addrs.append(addr.address)

        ip_str = "\n".join(ip_addrs) if ip_addrs else "Pas d'adresse IP"
        
        display_name = name
        if desc and desc.lower() != name.lower():
            display_name = f"{name}\n  -> {desc}"

        io = io_counters.get(name)
        if io:
            sent_mb = io.bytes_sent / (1024**2)
            recv_mb = io.bytes_recv / (1024**2)
            stats_str = f"DL : {recv_mb:.2f} MB\nUP : {sent_mb:.2f} MB"
        else:
            stats_str = "DL : 0.00 MB\nUP : 0.00 MB"

        results.append({
            "name": display_name,
            "stats": stats_str,
            "ips": ip_str
        })
    return results

def run_ping(ip: str, timeout: float = None) -> float:
    """Ping ICMP d'un hôte ; latence en ms, None (timeout) ou False (inconnu).

    `timeout` (secondes) borne l'attente de la réponse. Laissé à None, le délai
    par défaut de ping3 (4 s) s'applique. Pour les sondes de fond, on passe un
    timeout court (< 1 s) afin de ne pas retarder le planificateur sur un hôte
    injoignable.
    """
    if timeout is None:
        return ping(ip, unit='ms')
    return ping(ip, unit='ms', timeout=timeout)

def check_tcp_port(host: str, port: int, timeout: float = 2.0):
    """Teste l'ouverture d'un port TCP : tente une connexion à host:port.

    Renvoie la latence d'établissement de la connexion en millisecondes (float) si
    le port accepte la connexion, sinon None (port fermé, hôte injoignable, délai
    dépassé). Ne lève jamais. Signal fiable de joignabilité applicative, plus
    robuste que l'ICMP (souvent filtré par les pare-feux Windows). Style aligné
    sur run_ping (latence en ms ou rien)."""
    if not host or port is None:
        return None
    try:
        port = int(port)
    except (TypeError, ValueError):
        return None
    if not (0 < port < 65536):
        return None
    start = datetime.datetime.now()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError:
        return None
    elapsed = (datetime.datetime.now() - start).total_seconds() * 1000.0
    return round(elapsed, 2)

def get_system_counts() -> dict:
    """Retourne le nombre total de processus, de threads et de handles sur le système"""
    num_processes = 0
    num_threads = 0
    num_handles = 0
    for proc in psutil.process_iter(['num_threads']):
        try:
            num_processes += 1
            threads = proc.info['num_threads']
            if threads:
                num_threads += threads
            if platform.system() == 'Windows':
                try:
                    handles = proc.num_handles()
                    if handles:
                        num_handles += handles
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return {
        "processes": num_processes,
        "threads": num_threads,
        "handles": num_handles
    }

def get_processes_info(targets: list, delay: float = 0.1) -> list:
    """Retourne les informations de CPU, mémoire et ports d'écoute TCP pour les processus cibles"""
    matched_procs = []
    for proc in psutil.process_iter(['pid', 'name', 'memory_info', 'memory_percent']):
        try:
            name = proc.info['name']
            if name and any(t.strip().lower() in name.lower() for t in targets):
                proc.cpu_percent(interval=None)
                matched_procs.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    if not matched_procs:
        return []

    import time
    time.sleep(delay)

    results = []
    num_cores = psutil.cpu_count() or 1
    for proc in matched_procs:
        try:
            cpu = proc.cpu_percent(interval=None)
            global_cpu = cpu / num_cores
            mem_info = proc.info['memory_info']
            rss_mb = round(mem_info.rss / (1024**2), 2) if mem_info else 0.0

            # Récupérer les ports TCP en écoute (serveur)
            ports = []
            try:
                conns = proc.net_connections(kind='tcp')
                for conn in conns:
                    if conn.status == 'LISTEN':
                        ports.append(conn.laddr.port)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            ports = sorted(list(set(ports)))

            results.append({
                "pid": proc.info['pid'],
                "name": proc.info['name'],
                "cpu": global_cpu,
                "mem": rss_mb,
                "mem_percent": proc.info['memory_percent'],
                "ports": ports
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return results

def get_socket(cible: str = "ALL") -> list:
    """Retourne la liste des ports TCP en écoute et UDP ouverts filtrés par processus"""
    results = []
    filter_targets = []
    if cible.upper() != "ALL":
        filter_targets = [t.strip().lower() for t in cible.split(",") if t.strip()]

    try:
        conns = psutil.net_connections(kind='inet')
    except (psutil.AccessDenied, Exception):
        return []

    pid_names = {}
    for conn in conns:
        # Ignorer les sockets de type localhost/loopback (inaccessibles de l'extérieur)
        if conn.laddr:
            ip = conn.laddr.ip
            if ip == "::1" or ip.startswith("127.") or ip == "localhost":
                continue

        is_entry = False
        proto = ""
        if conn.type == socket.SOCK_STREAM and conn.status == 'LISTEN':
            is_entry = True
            proto = "TCP"
        elif conn.type == socket.SOCK_DGRAM and conn.laddr:
            is_entry = True
            proto = "UDP"

        if is_entry:
            pid = conn.pid
            proc_name = "-"
            if pid:
                if pid not in pid_names:
                    try:
                        proc = psutil.Process(pid)
                        pid_names[pid] = proc.name()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pid_names[pid] = "Inconnu"
                proc_name = pid_names[pid]

            if filter_targets:
                if not any(t in proc_name.lower() for t in filter_targets):
                    continue

            laddr_str = f"{conn.laddr.ip}:{conn.laddr.port}"
            results.append({
                "proto": proto,
                "port": conn.laddr.port,
                "ip": conn.laddr.ip,
                "laddr": laddr_str,
                "pid": pid if pid else "-",
                "proc_name": proc_name
            })

    results.sort(key=lambda x: (x["proto"], x["port"]))
    return results

def wrap_ports(ports: list, max_per_line: int = 4) -> str:
    """Met en forme les listes de ports en insérant des retours à la ligne toutes les N valeurs"""
    if not ports:
        return "-"
    chunks = [ports[i:i + max_per_line] for i in range(0, len(ports), max_per_line)]
    return "\n".join(", ".join(map(str, chunk)) for chunk in chunks)

def wrap_ips(ips: list) -> str:
    """Met en forme les IP (2 IPv4 par ligne max, 1 IPv6 par ligne max)"""
    if not ips:
        return "-"
    ipv4s = []
    ipv6s = []
    for ip in ips:
        if ":" in ip:
            ipv6s.append(ip)
        else:
            ipv4s.append(ip)

    lines = []
    # 2 IPv4 par ligne
    for i in range(0, len(ipv4s), 2):
        chunk = ipv4s[i:i + 2]
        lines.append(", ".join(chunk))
    # 1 IPv6 par ligne
    for ip in ipv6s:
        lines.append(ip)
    return "\n".join(lines)

# ==============================================================================
# PARTIE 2 : SECTION MAIN (AFFICHAGE CLI)
# ==============================================================================

if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(description="Affiche l'état du système sous forme de tableaux.")
    parser.add_argument(
        "-c", "--cible",
        type=str,
        help="Noms des processus à surveiller, séparés par des virgules (ex: 'chrome,python')"
    )
    args = parser.parse_args()

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

    def display_header(title):
        safe_print(f"\n{'='*20} {title} {'='*20}")

    # --- Affichage OS & Système ---
    display_header("SYSTEME INFO")
    os_info = [
        ["Date/Heure", get_now_datetime()],
        ["Dernier Boot", get_boot_time()],
        ["OS", f"{get_os_name()} ({get_os_version()})"]
    ]
    print_table(os_info, headers=["Indicateur", "Valeur"])

    # --- Affichage CPU & RAM (Global) ---
    display_header("RESSOURCES (GLOBAL)")
    cpu_data = get_cpu("all")
    mem_data = get_mem("all")
    sys_counts = get_system_counts()
    cores_str = f"Cœurs physiques: {cpu_data['coeurs_phys']} | Cœurs logiques: {cpu_data['coeurs_logiq']}"
    perf_data = [
        ["CPU (Usage)", f"{cpu_data['usage_global']}%", cores_str],
        ["RAM", f"{mem_data['percent']}%", f"{mem_data['available_gb']} GB dispo / {mem_data['total_gb']} GB"],
        ["Tâches Actives", f"{sys_counts['processes']}", f"Threads: {sys_counts['threads']} | Handles: {sys_counts['handles']}"]
    ]
    print_table(perf_data, headers=["Composant", "Usage", "Détails"])

    # --- Stockage ---
    display_header("STOCKAGE")
    storage_list = []
    for part in get_storage_partitions():
        try:
            d = get_disk_usage(part.mountpoint)
            storage_list.append([d['path'], f"{d['percent']}%", f"{d['free']} GB", f"{d['total']} GB"])
        except: continue
    print_table(storage_list, headers=["Lecteur", "Utilisé", "Libre", "Total"])

    # --- Interfaces Réseau & VPN ---
    display_header("INTERFACES RESEAU & VPN")
    vpn_intfs = get_vpn_interfaces()
    if vpn_intfs:
        # Tri par nom d'adaptateur
        vpn_intfs.sort(key=lambda x: x["name"].lower())
        table_data = []
        for v in vpn_intfs:
            table_data.append([
                v["name"],
                v["stats"],
                v["ips"]
            ])
        print_table(table_data, headers=["Nom de l'adaptateur", "Stats", "Adresse(s) IP"])
    else:
        safe_print("Aucune interface réseau détectée.")

    # --- Ports en écoute (Serveur) ---
    display_header("PORTS EN ECOUTE (SERVEUR)")
    socket_cible = args.cible if args.cible else "ALL"
    sockets = get_socket(socket_cible)
    if sockets:
        # Regroupement par nom de processus
        grouped = {}
        for s in sockets:
            name = s["proc_name"]
            if name not in grouped:
                grouped[name] = []
            grouped[name].append(s)

        table_data = []
        for name, group_sockets in grouped.items():
            # Déterminer les PIDs et le libellé PID
            pids = set(s["pid"] for s in group_sockets if s["pid"] != "-")
            if len(pids) > 1:
                pid_str = f"{len(pids)} Taches"
            elif len(pids) == 1:
                pid_str = str(list(pids)[0])
            else:
                pid_str = "-"

            # Collecter les ports TCP uniques
            tcp_ports = sorted(list(set(s["port"] for s in group_sockets if s["proto"] == "TCP")))
            tcp_str = wrap_ports(tcp_ports, 4)

            # Collecter les ports UDP uniques
            udp_ports = sorted(list(set(s["port"] for s in group_sockets if s["proto"] == "UDP")))
            udp_str = wrap_ports(udp_ports, 4)

            # Collecter les adresses d'écoute IP uniques
            ips = sorted(list(set(s["ip"] for s in group_sockets)))
            ips_str = wrap_ips(ips)

            table_data.append([
                pid_str,
                name,
                tcp_str,
                udp_str,
                ips_str
            ])

        # Tri par nom de processus
        table_data.sort(key=lambda x: x[1].lower())

        print_table(table_data, headers=["PID", "Processus", "Ports TCP", "Ports UDP", "Adresse d'écoute"])
    else:
        safe_print("Aucun port en écoute détecté.")

    # --- Connexions Actives (ESTABLISHED) ---
    display_header("CONNEXIONS ACTIVES (TCP ESTABLISHED)")
    active_cible = args.cible if args.cible else "ALL"
    active_conns = get_active_connections(active_cible)
    if active_conns:
        # Tri par nom de processus
        active_conns.sort(key=lambda x: x["proc_name"].lower())
        table_data = []
        for c in active_conns:
            table_data.append([
                c["pid"],
                c["proc_name"],
                c["laddr"],
                c["raddr"]
            ])
        print_table(table_data, headers=["PID", "Processus", "Adresse Locale", "Adresse Distante"])
    else:
        safe_print("Aucune connexion TCP active (ESTABLISHED) détectée.")

    # --- Test Ping ---
    display_header("TEST PING")
    ping_val = run_ping("8.8.8.8")
    ping_str = f"{round(ping_val, 2)} ms" if ping_val else "TIMEOUT"
    ping_table = [
        ["Ping Google (8.8.8.8)", ping_str]
    ]
    print_table(ping_table, headers=["Test", "Résultat"])

    # --- Exemple LISTALL (Top 5 Processus par CPU) ---
    display_header("TOP 5 PROCESSUS (CPU)")
    all_procs = get_cpu("LISTALL")["processes"]

    # 1. Grouper par nom en excluant l'Idle Process (PID 0)
    grouped_procs = {}
    for proc in all_procs:
        pid = proc.get('pid')
        if pid == 0:
            continue
        name = proc.get('name')
        if name not in grouped_procs:
            grouped_procs[name] = []
        grouped_procs[name].append(proc)

    # 2. Cumuler les valeurs
    aggregated_procs = []
    for name, instances in grouped_procs.items():
        total_cpu = sum(inst.get('cpu_percent', 0.0) for inst in instances)
        total_mem_rss = sum(inst.get('memory_rss_mb', 0.0) for inst in instances)
        total_mem_pct = sum(inst.get('memory_percent', 0.0) for inst in instances if inst.get('memory_percent') is not None)

        if len(instances) > 1:
            pid_str = f"{len(instances)} Taches"
        else:
            pid_str = str(instances[0].get('pid'))

        username = instances[0].get('username') or "-"

        aggregated_procs.append({
            "pid": pid_str,
            "name": name,
            "username": username,
            "cpu_percent": total_cpu,
            "memory_rss_mb": total_mem_rss,
            "memory_percent": total_mem_pct
        })

    # 3. Trier par CPU descendant et prendre le Top 5
    top_cpu = sorted(aggregated_procs, key=lambda x: x['cpu_percent'], reverse=True)[:5]

    top_cpu_data = []
    for proc in top_cpu:
        mem_rss = proc.get('memory_rss_mb', 0.0)
        mem_pct = proc.get('memory_percent', 0.0)
        top_cpu_data.append([
            proc.get('pid'),
            proc.get('name'),
            proc.get('username'),
            f"{proc.get('cpu_percent', 0.0):.1f}%",
            f"{mem_rss:.2f} MB ({mem_pct:.1f}%)"
        ])
    print_table(top_cpu_data, headers=["PID", "Nom du processus", "Utilisateur", "CPU %", "Mémoire"])

    # --- Section optionnelle pour les processus ciblés ---
    if args.cible:
        display_header("PROCESSUS CIBLES")
        targets = [t.strip() for t in args.cible.split(",") if t.strip()]
        proc_info = get_processes_info(targets)
        if proc_info:
            # Regroupement par nom de processus
            grouped = {}
            for p in proc_info:
                name = p["name"]
                if name not in grouped:
                    grouped[name] = []
                grouped[name].append(p)

            # Cumul des informations
            grouped_data = []
            for name, instances in grouped.items():
                total_cpu = sum(inst["cpu"] for inst in instances)
                total_mem = sum(inst["mem"] for inst in instances)
                total_mem_percent = sum(inst["mem_percent"] for inst in instances if inst["mem_percent"] is not None)

                # Récupérer l'ensemble des ports uniques
                all_ports = []
                for inst in instances:
                    all_ports.extend(inst["ports"])
                unique_ports = sorted(list(set(all_ports)))
                ports_str = wrap_ports(unique_ports, 4)

                if len(instances) > 1:
                    pid_str = f"{len(instances)} Taches"
                else:
                    pid_str = str(instances[0]["pid"])

                grouped_data.append({
                    "pid": pid_str,
                    "name": name,
                    "cpu": total_cpu,
                    "mem": total_mem,
                    "mem_percent": total_mem_percent,
                    "ports": ports_str
                })

            # Tri décroissant sur l'usage CPU, puis la mémoire
            grouped_data.sort(key=lambda x: (x['cpu'], x['mem']), reverse=True)

            table_data = []
            for gd in grouped_data:
                table_data.append([
                    gd["pid"],
                    gd["name"],
                    f"{gd['cpu']:.1f}%",
                    f"{gd['mem']:.2f} MB ({gd['mem_percent']:.1f}%)",
                    gd["ports"]
                ])
            print_table(table_data, headers=["PID", "Nom du processus", "CPU %", "Mémoire", "Ports d'écoute (TCP)"])
        else:
            safe_print("Aucun processus ne correspond aux cibles spécifiées.")
