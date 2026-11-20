
import socket
import threading
import mariadb
import traceback
import time

# --------------------------------------------------
# Configuration
# --------------------------------------------------

from config import HOST, PORT, LEASE_DAYS, DB_CONFIG


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
    cursor = conn.cursor()

    try:
        conn.begin()

        cursor.execute("""
            SELECT runid
            FROM runs
            WHERE status = 'pending'
               OR (status = 'assigned' AND lease_until <= NOW())
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

        print(
            f"Workunit {runid} attribué à "
            f"{username}/{hostname}"
        )

        return runid

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()


# --------------------------------------------------
# Confirmation d'un workunit terminé
# --------------------------------------------------

def complete_workunit(runid, username, hostname):

    conn = db_connect()
    cursor = conn.cursor()

    try:
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
        cursor.execute(f"""
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
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()


# --------------------------------------------------
# Communication avec un client
# --------------------------------------------------

def send_message(stream, message):

    stream.write((message + "\n").encode("utf-8"))
    stream.flush()


def handle_client(conn, address):

    print(f"Connexion entrante : {address}")

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

    print(f"PrimeGaps server v1.0 en écoute sur {HOST}:{PORT}")

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
