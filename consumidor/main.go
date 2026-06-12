package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	pb "discopass/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Parámetros por consola para crear distintos clientes
	nombreUsuario := flag.String("nombre", "Cliente A", "Nombre del usuario")
	medioPago := flag.String("pago", "debito", "Medio de pago: debito o credito")
	flag.Parse()

	// 1. Conectar al Broker
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("No se pudo conectar al Broker: %v", err)
	}
	defer conn.Close()
	client := pb.NewBrokerServiceClient(conn)

	// 2. Registro Inicial (Fase 1)
	ctxReg, cancelReg := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelReg()
	_, err = client.RegisterEntity(ctxReg, &pb.RegisterRequest{
		EntityId:   *nombreUsuario,
		EntityType: "CONSUMER",
	})
	if err != nil {
		log.Fatalf("Error al registrar %s: %v", *nombreUsuario, err)
	}
	log.Printf("[%s] Registrado en el sistema.\n", *nombreUsuario)

	// Darle un par de segundos para asegurar que haya eventos publicados
	time.Sleep(3 * time.Second)

	// 3. Consulta de Eventos Disponibles (Fase 4)
	log.Printf("[%s] Solicitando cartelera de eventos...\n", *nombreUsuario)
	ctxCartelera, cancelCartelera := context.WithTimeout(context.Background(), 5*time.Second)
	resCartelera, err := client.GetAvailableEvents(ctxCartelera, &pb.EmptyRequest{})
	cancelCartelera()

	if err != nil || len(resCartelera.GetEvents()) == 0 {
		log.Fatalf("[%s] No hay eventos disponibles en este momento.\n", *nombreUsuario)
	}

	// 4. Selección de Evento Aleatorio
	eventos := resCartelera.GetEvents()
	eventoElegido := eventos[rand.Intn(len(eventos))]
	log.Printf("[%s] Intentando comprar entrada para: %s (Precio: $%d, Stock: %d)\n",
		*nombreUsuario, eventoElegido.GetNombreEvento(), eventoElegido.GetPrecio(), eventoElegido.GetStock())

	// 5. Solicitud de Compra
	ctxCompra, cancelCompra := context.WithTimeout(context.Background(), 5*time.Second)
	resCompra, err := client.BuyTicket(ctxCompra, &pb.BuyRequest{
		UsuarioId: *nombreUsuario,
		EventoId:  eventoElegido.GetEventoId(),
		MedioPago: *medioPago,
	})
	cancelCompra()

	if err != nil {
		log.Fatalf("[%s] Error de comunicación al comprar: %v\n", *nombreUsuario, err)
	}

	// 6. Recepción de Ticket y Almacenamiento Local (CSV)
	if resCompra.GetSuccess() {
		ticketId := resCompra.GetTicketId()
		log.Printf("[%s] ¡Compra EXITOSA! Ticket ID: %s\n", *nombreUsuario, ticketId)

		// Guardar en archivo CSV propio
		nombreArchivo := fmt.Sprintf("%s.csv", *nombreUsuario)
		archivo, err := os.OpenFile(nombreArchivo, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Error al abrir archivo CSV: %v\n", err)
		}
		defer archivo.Close()

		escritorCSV := csv.NewWriter(archivo)
		// Si es un archivo nuevo, podríamos escribir los encabezados, pero escribiremos directamente los datos
		datosTicket := []string{ticketId, eventoElegido.GetEventoId(), eventoElegido.GetNombreEvento(), time.Now().Format(time.RFC3339)}
		escritorCSV.Write(datosTicket)
		escritorCSV.Flush()

		log.Printf("[%s] Ticket guardado en %s\n", *nombreUsuario, nombreArchivo)
	} else {
		log.Printf("[%s] Compra RECHAZADA: %s\n", *nombreUsuario, resCompra.GetMessage())
	}
}
