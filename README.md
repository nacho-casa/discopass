# Laboratorio 2: Sistemas Distribuidos - DiscoPass

Este repositorio contiene la implementación del Laboratorio 2 de la asignatura Sistemas Distribuidos. El sistema consiste en un ecosistema distribuido de venta y validación de entradas para discotecas (DiscoPass), coordinado por un Broker Central, con validación de pagos a través de una simulación del Banco USM y un sistema de almacenamiento replicado que garantiza consistencia y tolerancia a fallos mediante quórums (N=3, W=2, R=2).

### Integrantes (Grupo 20)
- Ignacio Casanova | Rol: 202273631-3  
- Mauro Castillo | Rol: 202273627-5
- Nicolás Ortíz | Rol: 202273528-7


## Características de la Implementación 

El sistema implementa las siguientes funcionalidades principales:

- *Almacenamiento Distribuido (N=3, W=2, R=2):* Replicación de datos con tolerancia a fallos y resincronización automática de nodos caídos.
- *Broker Central:* Autenticación de entidades, validación de eventos, quórum de lectura/escritura, gestión de compras y generación del Reporte.txt al finalizar.
- *Productores (Discotecas):* Publicación periódica de eventos mediante gRPC, asegurando idempotencia con identificadores únicos.
- *Consumidores:* Flujo de compra, almacenamiento local de tickets en archivos CSV y recuperación del historial ante desconexiones.
- *Banco USM:* Validación de pagos probabilística (80% general, 90% crédito) con manejo de caídas por parte del Broker.
- *Arquitectura:* Orquestación completa en contenedores Docker mediante Makefile, comunicándose exclusivamente a través de gRPC y Protocol Buffers.

---

## Instrucciones de Ejecución

Es necesario estar situado en la raíz del proyecto para ejecutar los comandos del Makefile.


### Orden de Inicialización (MVs)
Para cumplir con los quórums y dependencias, las máquinas deben levantarse en el siguiente orden:

1. *MV1 (Broker Central):*
  
   make docker-VM1
   
2. *MV4 (Banco USM + Nodo DB1):*
   
   make docker-VM4
   
   (Dar unos 5 segundos de margen para que se levanten los servidores).

3. *MV2 (Productores + Nodo DB3):*
   
   make docker-VM2
   
    (Dar unos 20 segundos de margen)
4. *MV3 (Consumidores + Nodo DB2):*
   
   make docker-VM3
   
   

---

## Monitoreo y Visualización

Los procesos se levantan en segundo plano . Para ver la salida de consola y la interacción entre entidades, se debe abrir otra terminal en la raíz del proyecto y ejecutar:

make logs

Para salir de los logs, presionar Ctrl + C (el sistema seguirá funcionando).

---

## Apagado Seguro y Reportes

Para finalizar la ejecución del laboratorio de forma correcta, NO se deben detener los contenedores manualmente. Se debe ejecutar el siguiente comando en la raíz del proyecto:

make clean

Esto envía una señal al Broker Central, el cual realizará lo siguiente:
1. Recolectar las estadísticas de todo el sistema.
2. Escribir el archivo Reporte.txt con los resultados requeridos en la rúbrica.
3. Notificar a los clientes para que guarden sus archivos .csv.
4. Bajar y limpiar los contenedores de Docker.

---

## Pruebas de Tolerancia a Fallos

El sistema soporta desconexiones. Se pueden probar abriendo otra terminal y ejecutando comandos de Docker:

- *Caída y recuperación de BD:*
  
  docker stop nodo_db2
  
  El sistema seguirá operando validando con DB1 y DB3 (W=2, R=2).
 
  docker start nodo_db2
  
  El nodo pedirá la información faltante a los otros nodos para sincronizarse.

- *Caída del Banco:*
  
  docker stop banco_usm
  
  El Broker no se cae; arroja un timeout y rechaza la compra sin afectar el inventario. Se puede volver a iniciar con docker start banco_usm.

Nota: Al finalizar la revisión en máquinas virtuales, recordar usar exit en cada terminal para cerrar correctamente las sesiones SSH.
