
import socket
import threading
import mariadb
import traceback
import time

# --------------------------------------------------
# Configuration
# --------------------------------------------------

from config import (
    HOST,
    PORT,
    LEASE_DAYS,
    DB_CONFIG,
    POOL_TRIGGER_FACTOR,
    POOL_TARGET_FACTOR,
    POOL_LOCK_NAME,
)


# --------------------------------------------------
# État global du gestionnaire de pool
# --------------------------------------------------

# Connexions TCP des clients correctement identifiés
active_connections = set()

# Synchronisation de l'état partagé
pool_state_lock = threading.Lock()

# Nombre d'attributions depuis le dernier contrôle
allocations_since_check = 0

# Réveille le gestionnaire lorsqu'un contrôle est nécessaire
pool_event = threading.Event()



# --------------------------------------------------
# Suivi des connexions et des attributions
# --------------------------------------------------

def register_connection(conn):
    """Enregistre une connexion TCP après identification."""
    with pool_state_lock:
        active_connections.add(conn)
        count = len(active_connections)
        pool_event.set()

    print(f"Connexions clientes actives : {count}")


def unregister_connection(conn):
    """Retire une connexion TCP à sa déconnexion."""
    with pool_state_lock:
        active_connections.discard(conn)
        count = len(active_connections)
        pool_event.set()

    print(f"Connexions clientes actives : {count}")


def register_allocation():
    """Compte une attribution et déclenche un contrôle si nécessaire."""
    global allocations_since_check

    with pool_state_lock:
        allocations_since_check += 1

        active_count = len(active_connections)
        threshold = active_count * POOL_TRIGGER_FACTOR

        if threshold > 0 and allocations_since_check >= threshold:
            pool_event.set()


# --------------------------------------------------
# Connexion MariaDB
# --------------------------------------------------

def db_connect():
    return mariadb.connect(**DB_CONFIG)


# --------------------------------------------------
# Attribution d'un workunit
# --------------------------------------------------


def get_workunit(username, hostname):

    conn = db_connect()
    cursor = None

    try:
        cursor = conn.cursor()
        conn.begin()

        cursor.execute("""
            SELECT runid
            FROM runs
            WHERE status = 'pending'
            ORDER BY runid
            LIMIT 1
            FOR UPDATE
        """)

        row = cursor.fetchone()

        if row is None:
            conn.commit()
            return None

        runid = row[0]

        cursor.execute(f"""
            UPDATE runs
            SET status = 'assigned',
                `user` = ?,
                host = ?,
                assigned_at = NOW(),
                completed_at = NULL,
                lease_until = DATE_ADD(NOW(), INTERVAL {LEASE_DAYS} DAY)
            WHERE runid = ?
        """, (username, hostname, runid))

        conn.commit()

        # Comptabiliser uniquement une attribution validée.
        register_allocation()

        print(
            f"Workunit {runid} attribué à "
            f"{username}/{hostname}"
        )

        return runid

    except Exception:
        try:
            conn.rollback()
        except Exception as rollback_error:
            print(
                f"Erreur rollback get_workunit : {rollback_error}"
            )
        raise

    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception as close_error:
                print(
                    f"Erreur fermeture curseur get_workunit : {close_error}"
                )

        try:
            conn.close()
        except Exception as close_error:
            print(
                f"Erreur fermeture connexion get_workunit : {close_error}"
            )


# --------------------------------------------------
# Confirmation d'un workunit terminé
# --------------------------------------------------


def complete_workunit(runid, username, hostname):

    conn = db_connect()
    cursor = None

    try:
        cursor = conn.cursor()
        conn.begin()

        cursor.execute("""
            SELECT status, `user`, host
            FROM runs
            WHERE runid = ?
            FOR UPDATE
        """, (runid,))

        row = cursor.fetchone()

        if row is None:
            conn.commit()
            return False

        status, assigned_user, assigned_host = row

        # Vérification de l'identité du client
        if assigned_user != username or assigned_host != hostname:
            conn.commit()
            return False

        # Confirmation déjà effectuée : réponse idempotente
        if status == "completed":
            conn.commit()
            return True

        if status != "assigned":
            conn.commit()
            return False

        # Vérification de l'existence du résultat dans gaps
        cursor.execute("""
            SELECT 1
            FROM gaps
            WHERE `run` = ?
              AND `user` = ?
              AND `host` = ?
            LIMIT 1
        """, (runid, username, hostname))

        result = cursor.fetchone()

        if result is None:
            conn.commit()
            return False

        # Validation définitive du workunit
        cursor.execute("""
            UPDATE runs
            SET status = 'completed',
                completed_at = NOW(),
                lease_until = NULL
            WHERE runid = ?
              AND status = 'assigned'
        """, (runid,))

        success = cursor.rowcount == 1

        conn.commit()

        if success:
            print(
                f"Workunit {runid} terminé par "
                f"{username}/{hostname}"
            )

        return success

    except Exception:
        try:
            conn.rollback()
        except Exception as rollback_error:
            print(
                f"Erreur rollback complete_workunit : {rollback_error}"
            )
        raise

    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception as close_error:
                print(
                    f"Erreur fermeture curseur complete_workunit : {close_error}"
                )

        try:
            conn.close()
        except Exception as close_error:
            print(
                f"Erreur fermeture connexion complete_workunit : {close_error}"
            )


# --------------------------------------------------
# Gestion du pool commun de runids
# --------------------------------------------------

def check_pool(active_count):
    """Complète le pool jusqu'à la réserve cible."""
    if active_count <= 0:
        return

    target = active_count * POOL_TARGET_FACTOR

    conn = None
    cursor = None
    lock_acquired = False

    try:
        conn = db_connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT GET_LOCK(?, 0)",
            (POOL_LOCK_NAME,)
        )
        row = cursor.fetchone()

        if row is None or row[0] != 1:
            print("Pool : verrou MariaDB indisponible, contrôle reporté.")
            return False

        lock_acquired = True

        cursor.execute("""
            SELECT COUNT(*)
            FROM runs
            WHERE status = 'pending'
        """)
        pending_count = cursor.fetchone()[0]

        missing = target - pending_count

        print(
            f"Pool : {pending_count} pending, "
            f"cible {target}, "
            f"manquants {max(0, missing)}."
        )

        if missing <= 0:
            conn.commit()
            return True

        cursor.execute("SELECT MAX(runid) FROM runs")
        row = cursor.fetchone()
        next_runid = 0 if row[0] is None else row[0] + 1

        runids = [
            (next_runid + offset,)
            for offset in range(missing)
        ]

        cursor.executemany(
            "INSERT INTO runs (runid) VALUES (?)",
            runids
        )

        conn.commit()

        print(
            f"Pool : {missing} runids ajoutés, "
            f"de {next_runid} à {next_runid + missing - 1}."
        )

        return True

    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception as rollback_error:
                print(
                    f"Erreur rollback check_pool : {rollback_error}"
                )
        raise

    finally:
        if lock_acquired and cursor is not None:
            try:
                cursor.execute(
                    "SELECT RELEASE_LOCK(?)",
                    (POOL_LOCK_NAME,)
                )
                row = cursor.fetchone()

                if row is None or row[0] != 1:
                    print("Pool : échec de libération du verrou MariaDB.")

            except Exception as release_error:
                print(
                    f"Erreur libération verrou pool : {release_error}"
                )

        if cursor is not None:
            try:
                cursor.close()
            except Exception as close_error:
                print(
                    f"Erreur fermeture curseur check_pool : {close_error}"
                )

        if conn is not None:
            try:
                conn.close()
            except Exception as close_error:
                print(
                    f"Erreur fermeture connexion check_pool : {close_error}"
                )

def pool_manager():
    """Thread indépendant chargé de maintenir la réserve."""
    global allocations_since_check

    print("Gestionnaire de pool démarré.")

    while True:
        pool_event.wait()

        # Instantané cohérent : aucun changement de connexion ou
        # d'attribution ne peut intervenir entre clear() et le comptage.
        with pool_state_lock:
            pool_event.clear()
            active_count = len(active_connections)
            allocations_since_check = 0

        if active_count == 0:
            print("Pool : aucun client actif, contrôle ignoré.")
            continue

        try:
            success = check_pool(active_count)

            if not success:
                print("Pool : nouvelle tentative dans 5 secondes.")
                time.sleep(5)

                with pool_state_lock:
                    if active_connections:
                        pool_event.set()

        except Exception as e:
            print(f"Erreur gestionnaire de pool : {e}")
            traceback.print_exc()

            time.sleep(5)

            with pool_state_lock:
                if active_connections:
                    pool_event.set()

# --------------------------------------------------
# Communication avec un client
# --------------------------------------------------

def send_message(stream, message):

    stream.write((message + "\n").encode("utf-8"))
    stream.flush()


def handle_client(conn, address):

    print(f"Connexion entrante : {address}")
    registered = False

    try:
        with conn:
            with conn.makefile("rwb") as stream:

                # Identification initiale
                line = stream.readline()

                if not line:
                    return

                command = line.decode("utf-8").strip()
                parts = command.split()

                if len(parts) != 3 or parts[0] != "HELLO":
                    send_message(stream, "ERROR")
                    return

                username = parts[1]
                hostname = parts[2]

                if len(username) > 64 or len(hostname) > 64:
                    send_message(stream, "ERROR")
                    print("Identification refusée : identifiant trop long.")
                    return

                register_connection(conn)
                registered = True

                print(
                    f"Client identifié : "
                    f"{username}/{hostname}"
                )

                send_message(stream, "READY")

                # Boucle de traitement des demandes
                while True:

                    line = stream.readline()

                    if not line:
                        break

                    command = line.decode("utf-8").strip()
                    parts = command.split()

                    if command == "GET":

                        runid = get_workunit(
                            username,
                            hostname
                        )

                        if runid is None:
                            send_message(stream, "EMPTY")
                            print(
                                f"Aucun workunit disponible "
                                f"pour {username}/{hostname}"
                            )
                        else:
                            send_message(
                                stream,
                                f"RUN {runid}"
                            )

                    elif len(parts) == 2 and parts[0] == "DONE":

                        try:
                            runid = int(parts[1])

                            success = complete_workunit(
                                runid,
                                username,
                                hostname
                            )

                            if success:
                                send_message(stream, "OK")
                            else:
                                send_message(stream, "ERROR")

                        except ValueError:
                            send_message(stream, "ERROR")

                    else:
                        send_message(stream, "ERROR")

    except Exception as e:
        print(f"Erreur avec le client {address}: {e}")
        traceback.print_exc()

    finally:
        if registered:
            unregister_connection(conn)

        print(f"Déconnexion : {address}")


# --------------------------------------------------
# Programme principal
# --------------------------------------------------

def main():

    while True:
        try:
            conn = db_connect()
            conn.close()
            print("Connexion MariaDB reussie.")
            break
        except mariadb.Error as e:
            print(f"MariaDB indisponible : {e}. Nouvelle tentative dans 5 secondes...")
            time.sleep(5)


    # Démarrage du serveur TCP
    server = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    server.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    server.bind((HOST, PORT))
    server.listen()

    # Démarrage du gestionnaire de pool dans un thread indépendant.
    pool_thread = threading.Thread(
        target=pool_manager,
        name="primegaps-pool-manager",
        daemon=True
    )
    pool_thread.start()

    print(f"PrimeGaps server v1.1 en écoute sur {HOST}:{PORT}")

    try:
        while True:

            conn, address = server.accept()

            thread = threading.Thread(
                target=handle_client,
                args=(conn, address),
                daemon=True
            )

            thread.start()

    except KeyboardInterrupt:
        print("\nArrêt du serveur.")

    finally:
        server.close()


if __name__ == "__main__":
    main()
