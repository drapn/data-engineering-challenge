"""Create the local UI account only on first initialization."""
import json
import subprocess

users = json.loads(subprocess.check_output(['airflow', 'users', 'list', '--output', 'json'], text=True))
if not any(user['username'] == 'admin' for user in users):
    subprocess.run(['airflow', 'users', 'create', '--username', 'admin', '--password', 'admin',
                    '--firstname', 'Local', '--lastname', 'Admin', '--role', 'Admin',
                    '--email', 'admin@example.com'], check=True)
