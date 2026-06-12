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
	nombreUsuario := flag.String("nombre", "Cliente_A", "Nombre del usuario")
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

	// Darle un par de segundos para asegurar que haya eventos publicados por las discotecas
	time.Sleep(3 * time.Second)

	// CICLO DE COMPRAS MULTIPLES (Intentará comprar 3 veces en eventos distintos)
	intentosDeCompra := 3
	eventosComprados := make(map[string]bool) // Memoria para no repetir compras

	for i := 1; i <= intentosDeCompra; i++ {
		log.Printf("\n[%s] --- Iniciando intento de compra %d de %d ---", *nombreUsuario, i, intentosDeCompra)

		// 3. Consulta de Eventos Disponibles (Fase 4)
		ctxCartelera, cancelCartelera := context.WithTimeout(context.Background(), 5*time.Second)
		resCartelera, err := client.GetAvailableEvents(ctxCartelera, &pb.EmptyRequest{})
		cancelCartelera()

		if err != nil || len(resCartelera.GetEvents()) == 0 {
			log.Printf("[%s] No hay eventos disponibles en este momento. Deteniendo compras.\n", *nombreUsuario)
			break // Salir del ciclo si ya no hay stock general
		}

		eventos := resCartelera.GetEvents()

		// Filtrar la cartelera para dejar solo los eventos que NO ha comprado antes
		var eventosDisponibles []*pb.Event
		for _, ev := range eventos {
			if !eventosComprados[ev.GetEventoId()] {
				eventosDisponibles = append(eventosDisponibles, ev)
			}
		}

		// Si ya compró entradas para todos los eventos publicados, termina el ciclo
		if len(eventosDisponibles) == 0 {
			log.Printf("[%s] Ya se compraron entradas para todos los eventos disponibles. Finalizando compras.\n", *nombreUsuario)
			break
		}

		// 4. Selección de Evento Aleatorio (solo entre los que no ha comprado)
		eventoElegido := eventosDisponibles[rand.Intn(len(eventosDisponibles))]
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
			log.Printf("[%s] Error de comunicación al comprar: %v\n", *nombreUsuario, err)
			time.Sleep(2 * time.Second)
			continue
		}

		// 6. Recepción de Ticket y Almacenamiento Local (CSV)
		if resCompra.GetSuccess() {
			ticketId := resCompra.GetTicketId()
			log.Printf("[%s] ¡Compra EXITOSA! Ticket ID: %s\n", *nombreUsuario, ticketId)

			// Registrar el evento como comprado para no volver a elegirlo
			eventosComprados[eventoElegido.GetEventoId()] = true

			// Guardar en archivo CSV propio
			nombreArchivo := fmt.Sprintf("%s.csv", *nombreUsuario)

			// Verificamos si el archivo existe para escribir la cabecera
			info, errStat := os.Stat(nombreArchivo)

			archivo, err := os.OpenFile(nombreArchivo, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("Error al abrir archivo CSV: %v\n", err)
			}

			escritorCSV := csv.NewWriter(archivo)

			// Escribimos la cabecera si el archivo es nuevo
			if os.IsNotExist(errStat) || info.Size() == 0 {
				escritorCSV.Write([]string{"Ticket_ID", "Evento_ID", "Nombre_Evento", "Fecha_Hora"})
			}

			datosTicket := []string{ticketId, eventoElegido.GetEventoId(), eventoElegido.GetNombreEvento(), time.Now().Format(time.RFC3339)}
			escritorCSV.Write(datosTicket)
			escritorCSV.Flush()

			// Cerramos el archivo explícitamente en cada iteración
			archivo.Close()

			log.Printf("[%s] Ticket guardado en %s\n", *nombreUsuario, nombreArchivo)
		} else {
			log.Printf("[%s] Compra RECHAZADA: %s\n", *nombreUsuario, resCompra.GetMessage())
		}

		// Pausa de 2 segundos antes de intentar comprar de nuevo
		time.Sleep(2 * time.Second)
	}

	log.Printf("[%s] Ha finalizado su sesión de compras.\n", *nombreUsuario)
}
