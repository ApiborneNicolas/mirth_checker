# -*- coding: utf-8 -*-
"""
Support HTTPS : génération d'un certificat auto-signé et contexte SSL serveur.

Le serveur web en cours d'exécution n'utilise que le module standard `ssl`
(`build_ssl_context`). La génération d'un certificat auto-signé
(`ensure_self_signed_cert`) requiert la bibliothèque `cryptography` — utilisée
UNIQUEMENT pour créer le certificat au 1er démarrage (idéal pour un `.exe`
Windows autonome). Si `cryptography` est absente et qu'aucun certificat n'est
fourni, on lève une erreur claire invitant à fournir HTTPS_CERT/HTTPS_KEY ou à
installer `cryptography` (ou à générer le certificat via openssl, cf.
_cmd_helper/gen_cert.bat).
"""

import os
import ssl
import socket
import ipaddress
import datetime


def _local_hostname():
    try:
        return socket.gethostname() or "localhost"
    except OSError:
        return "localhost"


def ensure_self_signed_cert(cert_path, key_path, hostname=None):
    """Génère un certificat auto-signé (cert_path/key_path) s'il manque.

    Renvoie True si les fichiers existent (déjà présents ou fraîchement créés).
    Lève RuntimeError si la génération est nécessaire mais `cryptography` absente.
    """
    if os.path.isfile(cert_path) and os.path.isfile(key_path):
        return True

    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
    except ImportError as e:
        raise RuntimeError(
            "Génération du certificat auto-signé impossible : la bibliothèque "
            "'cryptography' est absente. Fournissez HTTPS_CERT/HTTPS_KEY (certificat "
            "existant) ou installez 'cryptography' (pip install cryptography), ou "
            "générez le certificat via _cmd_helper/gen_cert.bat (openssl)."
        ) from e

    host = hostname or _local_hostname()

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, host),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Mirth_checker"),
    ])

    # SAN : nom d'hôte + localhost + loopback, pour que l'accès local et par nom
    # fonctionne sans erreur de correspondance.
    san = [x509.DNSName(host), x509.DNSName("localhost"),
           x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
           x509.IPAddress(ipaddress.ip_address("::1"))]
    # Ajoute les IPv4 locales détectées (best-effort).
    try:
        for info in socket.getaddrinfo(host, None):
            addr = info[4][0]
            try:
                ipobj = ipaddress.ip_address(addr)
            except ValueError:
                continue
            if not any(isinstance(n, x509.IPAddress) and n.value == ipobj for n in san):
                san.append(x509.IPAddress(ipobj))
    except OSError:
        pass

    now = datetime.datetime.utcnow()
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=3650))   # ~10 ans
        .add_extension(x509.SubjectAlternativeName(san), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )

    # Écrit la clé privée (non chiffrée) puis le certificat, en PEM.
    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)

    with open(key_path, "wb") as f:
        f.write(key_pem)
    with open(cert_path, "wb") as f:
        f.write(cert_pem)
    # Restreint la clé au propriétaire quand la plateforme le permet (sans effet
    # notable sous Windows, inoffensif).
    try:
        os.chmod(key_path, 0o600)
    except OSError:
        pass
    return True


def build_ssl_context(cert_path, key_path):
    """Contexte SSL serveur chargé du certificat/clé (module standard `ssl`)."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
    return ctx
