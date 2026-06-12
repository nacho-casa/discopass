# Construir la imagen base de Docker
build:
	docker-compose build

# Levantar la Máquina Virtual 1 (Broker)
docker-VM1:
	docker-compose --profile vm1 up

# Levantar la Máquina Virtual 2 (Productores y DB3)
docker-VM2:
	docker-compose --profile vm2 up

# Levantar la Máquina Virtual 3 (Consumidores y DB2)
docker-VM3:
	docker-compose --profile vm3 up

# Levantar la Máquina Virtual 4 (Banco y DB1)
docker-VM4:
	docker-compose --profile vm4 up

# Detener todos los contenedores y limpiar
clean:
	docker-compose --profile vm1 --profile vm2 --profile vm3 --profile vm4 down