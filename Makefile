docker-VM1:
	docker-compose up -d --build broker

docker-VM2:
	docker-compose up -d --build productor_dataclub productor_dockers productor_golounge productor_georgiehouse db3

docker-VM3:
	docker-compose up -d --build consumidor_a consumidor_b db2

docker-VM4:
	docker-compose up -d --build banco db1

logs:
	docker-compose logs -f

clean:
	docker-compose down -v