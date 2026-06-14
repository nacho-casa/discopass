docker-VM1:
	sudo docker-compose up -d --build broker

docker-VM2:
	sudo docker-compose up -d --build productor_dataclub productor_dockers productor_golounge productor_georgiehouse db3

docker-VM3:
	sudo docker-compose up -d --build consumidor_a consumidor_b db2

docker-VM4:
	sudo docker-compose up -d --build banco db1

logs:
	sudo docker-compose logs -f

clean:
	sudo docker-compose down -v