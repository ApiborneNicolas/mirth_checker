# Mirth Checker & System Monitor

Ce projet est une boîte à outils en Python conçue pour l'analyse des fichiers de logs Mirth Connect, la surveillance des ressources système sous Windows, et la génération de rapports automatiques envoyés par email.

---

## 📋 Table des matières
1. [🚀 Description des scripts principaux](#-description-des-scripts-principaux)
2. [⚙️ Configuration SMTP sécurisée](#%EF%B8%8F-configuration-smtp-sécurisée)
3. [🛠️ Outils et Scripts d'aide (_cmd_helper)](#%EF%B8%8F-outils-et-scripts-daide-_cmd_helper)
4. [📦 Compilation des exécutables autonomes](#-compilation-des-exécutables-autonomes)
5. [📈 Dépendances requises](#-dépendances-requises)

---

## 🚀 Description des scripts principaux

Le projet comprend trois scripts Python majeurs, exécutables en ligne de commande ou via le menu interactif :

### 1. `mirth_logs_parser.py`
Analyseur et décodeur de logs pour Mirth Connect. 
* **Fonctionnalités** :
  * Regroupement des lignes multi-lignes appartenant à une même entrée de log.
  * Statistiques de répartition par niveau de logs (`INFO`, `ERROR`, `WARN`, etc.).
  * Répartition des logs par canaux (Channels) et détection automatique des IDs et connecteurs.
  * Identification des 10 threads et classes d'origine les plus actifs.
  * Extraction des erreurs et de leurs causes racines (`Caused by:`).
  * Possibilité d'envoyer le rapport formaté en HTML par email (via `quickmail.py`).

### 2. `system_state.py`
Outil de diagnostic et de monitoring pour Windows.
* **Fonctionnalités** :
  * Informations matérielles (CPU logique/physique, RAM totale/disponible, partitions disques).
  * Analyse de l'usage CPU global et des processus actifs consommant le plus de ressources.
  * Statistiques réseau (bande passante consommée, nombre de sockets TCP/UDP ouverts).
  * Statut de la connexion VPN (état actif/inactif des interfaces réseaux associées).

### 3. `quickmail.py`
Script utilitaire d'envoi rapide d'e-mails via un serveur SMTP (supporte TLS et SSL). Il est utilisé comme module par l'analyseur de logs, mais peut également être lancé directement en CLI.

---

## ⚙️ Configuration SMTP sécurisée

Pour le travail collaboratif sous Git, les identifiants SMTP de `quickmail.py` ont été extraits du code source :
* Le fichier de secrets réels s'appelle **`.smtp_config.py`** et est ignoré par Git (défini dans `.gitignore`).
* Un modèle nommé **`.smtp_config.py.template`** est fourni et suivi par Git.

### 📝 Configuration initiale :
Pour configurer vos identifiants, copiez le template et renseignez vos informations :
```bash
cp .smtp_config.py.template .smtp_config.py
```
Puis éditez `.smtp_config.py` :
```python
SMTP_SERVER = "smtp.votre-fournisseur.com"
SMTP_PORT = 587
USE_SSL = False
SMTP_USER = "votre_utilisateur"
SMTP_PASSWORD = "votre_mot_de_pass"
SENDER_ADDRESS = "votre_adresse@email.com"
```

> [!TIP]
> **Surcharge par variables d'environnement** :
> Les scripts supportent également la configuration via des variables d'environnement (ex: `SMTP_PASSWORD`, `SMTP_USER`, etc.). Si ces variables sont présentes dans le système, elles surchargeront les valeurs définies dans `.smtp_config.py`.

---

## 🛠️ Outils et Scripts d'aide (`_cmd_helper`)

Un ensemble de scripts Batch (`.bat`) est disponible dans le dossier `_cmd_helper/` pour faciliter le cycle de développement et d'utilisation :

| Script | Description |
| :--- | :--- |
| `venv_create.bat` | Crée automatiquement l'environnement virtuel Python (`venv`) à la racine. |
| `venv_load.bat` | Charge virtuellement l'environnement Python de manière silencieuse pour les autres scripts. |
| `update.bat` | Met à jour `pip` et installe/met à niveau toutes les dépendances définies dans `requirements.txt`. |
| `launch.bat` | **Menu interactif** de lancement. Il scanne le dossier racine, liste tous les scripts `.py` (en filtrant les fichiers de config commençant par un point comme `.smtp_config.py`), affiche l'aide syntaxique du script sélectionné, et demande à l'utilisateur d'entrer les arguments avant de le lancer. |
| `_compilation.bat` | Compile les scripts Python en fichiers exécutables autonomes (`.exe`). |

---

## 📦 Compilation des exécutables autonomes

Le script `_cmd_helper\_compilation.bat` compile les trois scripts majeurs sous forme d'exécutables autonomes (`dist/*.exe`) à l'aide de PyInstaller :

```powershell
_cmd_helper\_compilation.bat
```

> [!IMPORTANT]
> **Sécurité des secrets à la compilation** :
> Lors de la compilation, l'option `--add-data ".smtp_config.py;."` est passée à PyInstaller pour `quickmail.py` et `mirth_logs_parser.py`. Vos identifiants de connexion SMTP configurés dans votre fichier local `.smtp_config.py` sont donc **directement packagés à l'intérieur des fichiers `.exe` finaux**. Les exécutables ainsi générés sont 100% autonomes et n'ont pas besoin de fichier de configuration externe pour fonctionner sur les machines cibles.

---

## 📈 Dépendances requises

Les dépendances requises (gérées via `update.bat`) sont :
* `psutil` : pour l'accès aux statistiques système et processus.
* `tabulate` : pour le formatage propre des rapports sous forme de tableaux dans la console.
* `ping3` : pour tester la connectivité réseau.
* `pyinstaller` : (requis pour la compilation en `.exe`).
