import json
import os
import hashlib

USERS_FILE = "users.json"

def hash_password(password, salt=None):
    """
    パスワードにソルトを付与し、SHA-256で強固にハッシュ化します。
    """
    if not salt:
        salt = os.urandom(16).hex()
    hashed = hashlib.sha256((password + salt).encode("utf-8")).hexdigest()
    return hashed, salt

def verify_password(password, stored_hash, salt):
    """
    入力されたパスワードと保存されたハッシュ値を比較検証します。
    """
    check_hash, _ = hash_password(password, salt)
    return check_hash == stored_hash

def load_users():
    """
    users.json から登録ユーザー情報を読み込みます。
    """
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_users(users_data):
    """
    users.json にユーザー情報を安全に保存します。
    """
    try:
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(users_data, f, ensure_ascii=False, indent=4)
        return True
    except Exception:
        return False

def add_user(username, password):
    """
    新規ユーザーを追加登録します。重複チェックを行います。
    """
    users = load_users()
    username = username.strip()
    if not username or not password:
        return False, "ユーザーIDまたはパスワードが空です。"
    if username in users:
        return False, f"ユーザーID '{username}' は既に登録されています。"
    
    pwd_hash, salt = hash_password(password)
    users[username] = {
        "password_hash": pwd_hash,
        "salt": salt
    }
    
    if save_users(users):
        return True, "ユーザーを正常に登録しました。"
    return False, "データベースへの保存に失敗しました。"

def delete_user(username):
    """
    指定されたユーザーを削除します。
    """
    users = load_users()
    if username in users:
        del users[username]
        if save_users(users):
            return True, "ユーザーを正常に削除しました。"
        return False, "データベースの更新に失敗しました。"
    return False, f"ユーザーID '{username}' は見つかりませんでした。"

def authenticate_user(username, password):
    """
    一般ユーザーのログインを認証します。
    """
    users = load_users()
    username = username.strip()
    if username in users:
        user_info = users[username]
        stored_hash = user_info.get("password_hash")
        salt = user_info.get("salt")
        if stored_hash and salt:
            return verify_password(password, stored_hash, salt)
    return False

def change_user_password(username, new_password):
    """
    指定した一般ユーザーのパスワードを変更します。
    """
    users = load_users()
    username = username.strip()
    if username not in users:
        return False, f"ユーザーID '{username}' は存在しません。"
    if not new_password:
        return False, "パスワードが空です。"
        
    pwd_hash, salt = hash_password(new_password)
    users[username]["password_hash"] = pwd_hash
    users[username]["salt"] = salt
    
    if save_users(users):
        return True, f"ユーザー '{username}' のパスワードを正常に変更しました。"
    return False, "データベースの保存に失敗しました。"

ADMIN_CONFIG_FILE = "admin_config.json"

def load_admin_password():
    """
    管理者用パスワードのハッシュとソルトを取得します。
    """
    if os.path.exists(ADMIN_CONFIG_FILE):
        try:
            with open(ADMIN_CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None

def change_admin_password(new_password):
    """
    管理者用マスターパスワードを変更し、ハッシュ化して保存します。
    """
    if not new_password:
        return False, "パスワードが空です。"
    pwd_hash, salt = hash_password(new_password)
    config = {
        "password_hash": pwd_hash,
        "salt": salt
    }
    try:
        with open(ADMIN_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=4)
        return True, "管理者パスワードを正常に変更しました。"
    except Exception:
        return False, "設定ファイルの保存に失敗しました。"

def authenticate_admin(password):
    """
    管理者（開発者）のログイン認証を行います。
    """
    config = load_admin_password()
    if config:
        stored_hash = config.get("password_hash")
        salt = config.get("salt")
        if stored_hash and salt:
            return verify_password(password, stored_hash, salt)
            
    # フォールバック（初期設定）
    dev_master = os.environ.get("DEV_ADMIN_PASSWORD", "boatpredict-admin2026")
    return password == dev_master
