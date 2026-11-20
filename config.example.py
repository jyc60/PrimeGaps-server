HOST = "0.0.0.0"
PORT = 5000
LEASE_DAYS = 30

# Configuration exemple PrimeGaps Server

DB_CONFIG = {
    "host": "10.1.1.76",
    "user": "admin",
    "password": "A_REMPLACER",
    "database": "primegaps",
    "autocommit": False
}

# Gestion du pool commun - PrimeGaps Server V1.1
POOL_TRIGGER_FACTOR = 60
POOL_TARGET_FACTOR = 1728
POOL_LOCK_NAME = "primegaps_run_pool"
