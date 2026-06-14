# Laboratorio 2: Sistemas Distribuidos - DiscoPass

Este repositorio contiene la implementación del Laboratorio 2 de la asignatura Sistemas Distribuidos. El sistema simula un ecosistema distribuido de venta y validación de entradas para discotecas (DiscoPass), coordinado por un Broker Central, con validación de pagos a través del Banco USM y almacenamiento replicado tolerante a fallos garantizando consistencia (N=3, W=2, R=2).

### Integrantes (Grupo 20)
* Ignacio Casanova | Rol: Desarrollo Backend y Arquitectura Docker
* [Nombre Integrante 2] | Rol: [Especificar Rol]
* [Nombre Integrante 3] | Rol: [Especificar Rol]

### Instrucciones de ejecución

Antes de ejecutar los programas, es importante que en cada una de las terminales nos encontremos en la carpeta "Grupo-20" [Ejecutar `cd Grupo-20`] para el correcto funcionamiento del código y de los atajos del Makefile.

**Configuración de Red (Paso Crítico para despliegue real):**
Antes de levantar las máquinas virtuales de la universidad, es obligatorio editar el archivo `docker-compose.yml`. En cada uno de los servicios, se debe modificar la variable de entorno `BROKER_HOST=192.168.X.X:50051`, reemplazando la IP por la dirección exacta que tenga asignada la Máquina Virtual 1 (la que alojará al Broker Central). Si se ejecuta en local, el sistema detectará `localhost` por defecto.

La distribución de las máquinas virtuales para la correcta ejecución del código, respetando el orden de dependencias y el quórum, es la siguiente:
* **MV1:** Broker Central
* **MV4:** Banco USM + Nodo DB1
* **MV2:** Productores (Discotecas) + Nodo DB3
* **MV3:** Consumidores (Usuarios A y B) + Nodo DB2

Los comandos a ejecutar para cada MV serían los siguientes y estrictamente en este orden:

1. **MV1** => `make docker-VM1`
2. **MV4** => `make docker-VM4` [esperar unos 5 segundos antes de ejecutar el siguiente comando para asegurar que el servidor gRPC del banco y DB1 estén listos].
3. **MV2** => `make docker-VM2` [**IMPORTANTE:** Al iniciar esta máquina, el sistema alcanza el quórum de escritura (W=2). Los productores están configurados para esperar exactamente 15 segundos antes de enviar su primer evento para permitir que la red DB se estabilice y evitar "Condiciones de Carrera" con eventos huérfanos. Se recomienda esperar al menos 45 a 60 segundos en total antes de pasar a la siguiente MV para que la cartelera se llene].
4. **MV3** => `make docker-VM3` La ejecución de este comando hace que entren los clientes a comprar. Al iniciar, los consumidores consultan el historial previo (R=2) para evitar compras duplicadas en caso de reintegración. Además, entre cada intento de compra, los consumidores esperarán 20 segundos para permitir que la cartelera se actualice con nuevos eventos.

### Monitoreo y visualización
Como los contenedores se ejecutan en segundo plano (modo detached), si se desea ver en tiempo real lo que está ocurriendo con los eventos, validaciones, confirmaciones de stock o fallos de red, se debe abrir una terminal extra en la carpeta del proyecto y ejecutar el comando:
* `make logs`
Para salir de la vista de los logs basta con presionar la combinación de teclas "Ctrl + C" (esto solo cierra la visualización, el sistema seguirá corriendo de fondo).

### Apagado seguro y Reportes
Al momento en que se quiera detener la simulación de todo el sistema, **NO** se debe hacer matando los procesos uno por uno. En cualquier terminal ubicada en la carpeta del proyecto, se debe ejecutar el siguiente comando:
* `make clean`

La ejecución de `make clean` tiene como propósito enviar una señal de apagado seguro (Graceful Shutdown) al Broker Central. Al recibirla, el Broker consolidará todas las estadísticas y generará automáticamente el archivo `Reporte.txt` exigido en la pauta antes de destruir todos los contenedores y limpiar la red virtual. Además, en la raíz del proyecto quedarán generados los archivos `Cliente_A.csv` y `Cliente_B.csv` con los tickets de las compras exitosas.

### Pruebas de Tolerancia a Fallos
Para evaluar la resiliencia del sistema (W=2, R=2), durante la ejecución normal se pueden abrir terminales independientes y simular caídas de nodos utilizando comandos de Docker, por ejemplo:
* `docker stop nodo_db2` (Simula la caída de una base de datos. El sistema seguirá operando validando con DB1 y DB3).
* `docker start nodo_db2` (Simula la recuperación. El nodo se resincronizará automáticamente con sus pares para recuperar los datos perdidos).
* `docker stop banco_usm` (El broker arrojará timeouts, rechazará las compras temporalmente sin afectar el stock, y el sistema no colapsará).

Para finalizar, consideramos que es de buena práctica que al momento de querer salir de las MV se ejecute `exit` en cada una de sus correspondientes terminales, para cerrar las sesiones de ssh de manera segura.