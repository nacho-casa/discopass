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
	nombreUsuario := flag.String("nombre", "Cliente_A", "Nombre del usuario")
	medioPago := flag.String("pago", "debito", "Medio de pago: debito o credito")
	flag.Parse()

	brokerHost := os.Getenv("BROKER_HOST")
	if brokerHost == "" {
		brokerHost = "localhost:50051"
	}

	conn, err := grpc.NewClient(brokerHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("No se pudo conectar al Broker en %s: %v", brokerHost, err)
	}
	defer conn.Close()
	client := pb.NewBrokerServiceClient(conn)

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

	eventosComprados := make(map[string]bool)

	log.Printf("[%s] Consultando historial (R=2) por si es una reintegración tras caída...", *nombreUsuario)
	ctxHist, cancelHist := context.WithTimeout(context.Background(), 5*time.Second)
	resHist, errHist := client.GetUserHistory(ctxHist, &pb.HistoryRequest{UsuarioId: *nombreUsuario})
	cancelHist()

	if errHist == nil {
		ticketsRecuperados := resHist.GetTickets()
		if len(ticketsRecuperados) > 0 {
			log.Printf("[%s]  REINTEGRACIÓN DETECTADA: Se recuperaron %d compras del historial.", *nombreUsuario, len(ticketsRecuperados))
			for _, t := range ticketsRecuperados {
				eventosComprados[t.GetEventoId()] = true // Se anota para no duplicar compra
				log.Printf("      -> Ticket recuperado: %s (Evento: %s)", t.GetTicketId(), t.GetEventoId())
			}
		} else {
			log.Printf("[%s] Inicio en limpio: No hay historial de compras previo.", *nombreUsuario)
		}
	} else {
		log.Printf("[%s] Error consultando historial o BD vacía: %v", *nombreUsuario, errHist)
	}

	time.Sleep(3 * time.Second)

	intentosDeCompra := 5

	for i := 1; i <= intentosDeCompra; i++ {
		log.Printf("\n[%s] --- Iniciando intento de compra %d de %d ---", *nombreUsuario, i, intentosDeCompra)

		ctxCartelera, cancelCartelera := context.WithTimeout(context.Background(), 5*time.Second)
		resCartelera, err := client.GetAvailableEvents(ctxCartelera, &pb.EmptyRequest{})
		cancelCartelera()

		if err != nil || len(resCartelera.GetEvents()) == 0 {
			log.Printf("[%s] No hay eventos disponibles en la cartelera consensuada. Deteniendo compras.\n", *nombreUsuario)
			break
		}

		eventos := resCartelera.GetEvents()
		var eventosDisponibles []*pb.Event
		for _, ev := range eventos {

			if !eventosComprados[ev.GetEventoId()] {
				eventosDisponibles = append(eventosDisponibles, ev)
			}
		}

		if len(eventosDisponibles) == 0 {
			log.Printf("[%s] Ya se compraron entradas para todos los eventos disponibles. Finalizando para evitar duplicidad.\n", *nombreUsuario)
			break
		}

		eventoElegido := eventosDisponibles[rand.Intn(len(eventosDisponibles))]
		log.Printf("[%s] Intentando comprar entrada para: %s (Precio: $%d, Stock: %d)\n",
			*nombreUsuario, eventoElegido.GetNombreEvento(), eventoElegido.GetPrecio(), eventoElegido.GetStock())

		ctxCompra, cancelCompra := context.WithTimeout(context.Background(), 5*time.Second)
		resCompra, err := client.BuyTicket(ctxCompra, &pb.BuyRequest{
			UsuarioId: *nombreUsuario,
			EventoId:  eventoElegido.GetEventoId(),
			MedioPago: *medioPago,
		})
		cancelCompra()

		if err != nil {
			log.Printf("[%s] Error de comunicación al comprar: %v\n", *nombreUsuario, err)
			time.Sleep(2 * time.Second)
			continue
		}

		if resCompra.GetSuccess() {
			ticketId := resCompra.GetTicketId()
			log.Printf("[%s] ¡Compra EXITOSA! Ticket ID: %s\n", *nombreUsuario, ticketId)

			eventosComprados[eventoElegido.GetEventoId()] = true

			nombreArchivo := fmt.Sprintf("%s.csv", *nombreUsuario)
			info, errStat := os.Stat(nombreArchivo)

			archivo, err := os.OpenFile(nombreArchivo, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("Error al abrir archivo CSV: %v\n", err)
			}

			escritorCSV := csv.NewWriter(archivo)

			if os.IsNotExist(errStat) || info.Size() == 0 {
				escritorCSV.Write([]string{"Ticket_ID", "Evento_ID", "Nombre_Evento", "Fecha_Hora"})
			}

			datosTicket := []string{ticketId, eventoElegido.GetEventoId(), eventoElegido.GetNombreEvento(), time.Now().Format(time.RFC3339)}
			escritorCSV.Write(datosTicket)
			escritorCSV.Flush()

			archivo.Close()
			log.Printf("[%s] Ticket guardado en %s\n", *nombreUsuario, nombreArchivo)
		} else {
			log.Printf("[%s] Compra RECHAZADA: %s\n", *nombreUsuario, resCompra.GetMessage())
		}

		log.Printf("[%s] Esperando 20 segundos antes de intentar nuevamente...\n", *nombreUsuario)
		time.Sleep(20 * time.Second)
	}

	log.Printf("[%s] Ha finalizado su sesión de compras.\n", *nombreUsuario)
}
