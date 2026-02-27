#!/usr/bin/env python3
"""
Generate hashed credentials for the Streamlit auth system.

Run once, then paste the output into .streamlit/secrets.toml
(or Streamlit Cloud → App Settings → Secrets).

Usage:
    python generate_password_hash.py
"""
import getpass
import hashlib
import secrets


def main():
    print("Breeze Options Dashboard — Credential Generator")
    print("=" * 52)

    username = input("Username: ").strip()
    if not username:
        raise SystemExit("ERROR: Username cannot be empty.")

    password = getpass.getpass("Password: ")
    confirm  = getpass.getpass("Confirm password: ")

    if not password:
        raise SystemExit("ERROR: Password cannot be empty.")
    if password != confirm:
        raise SystemExit("ERROR: Passwords do not match.")

    salt          = secrets.token_hex(32)
    password_hash = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()

    print()
    print("Add these lines to your secrets.toml (or Streamlit Cloud Secrets):")
    print("-" * 52)
    print(f'AUTH_USERNAME      = "{username}"')
    print(f'AUTH_SALT          = "{salt}"')
    print(f'AUTH_PASSWORD_HASH = "{password_hash}"')
    print("-" * 52)
    print("Done. Keep secrets.toml out of version control (.gitignore already handles this).")


if __name__ == "__main__":
    main()
