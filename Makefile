.PHONY: all clean docker-VM1 docker-VM2 docker-VM3 docker-VM4

# ==========================================
# MÁQUINA VIRTUAL 1 (MV1)
# ==========================================
# Levanta el Broker Central
docker-VM1:
	docker compose --profile vm1 up -d --build

# ==========================================
# MÁQUINA VIRTUAL 2 (MV2)
# ==========================================
# Levanta los Productores (Discotecas) y el Nodo DB3
docker-VM2:
	docker compose --profile vm2 up -d --build

# ==========================================
# MÁQUINA VIRTUAL 3 (MV3)
# ==========================================
# Levanta los Consumidores (Cliente A y B) y el Nodo DB2
docker-VM3:
	docker compose --profile vm3 up -d --build

# ==========================================
# MÁQUINA VIRTUAL 4 (MV4)
# ==========================================
# Levanta el Banco USM y el Nodo DB1
docker-VM4:
	docker compose --profile vm4 up -d --build

# ==========================================
# COMANDO DE LIMPIEZA
# ==========================================
# Detiene todos los contenedores activos de todos los perfiles
clean:
	docker compose --profile vm1 --profile vm2 --profile vm3 --profile vm4 down

# ==========================================
# VER LOGS EN TIEMPO REAL
# ==========================================
# Muestra la salida en vivo de todos los contenedores activos
logs:
	docker compose --profile vm1 --profile vm2 --profile vm3 --profile vm4 logs -f
