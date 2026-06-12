package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"math/rand"
	"os"
	"time"

	pb "discopass/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type EventoJSON struct {
	EventoID         string `json:"evento_id"`
	Discoteca        string `json:"discoteca"`
	NombreEvento     string `json:"nombre_evento"`
	Categoria        string `json:"categoria"`
	Comuna           string `json:"comuna"`
	Precio           int32  `json:"precio"`
	Stock            int32  `json:"stock"`
	FechaEvento      string `json:"fecha_evento"`
	FechaPublicacion string `json:"fecha_publicacion"`
}

func main() {
	// Definir banderas para la terminal
	nombreDiscoteca := flag.String("nombre", "DataClub", "Nombre de la discoteca productora")
	archivoCatalogo := flag.String("catalogo", "catalogo_eventos_30.json", "Archivo JSON a leer")
	flag.Parse()

	// 1. Conectar al Broker
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("No se pudo conectar al Broker: %v", err)
	}
	defer conn.Close()
	client := pb.NewBrokerServiceClient(conn)

	// 2. Registrarse (Usando el nombre dinámico)
	ctxReg, cancelReg := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelReg()
	_, err = client.RegisterEntity(ctxReg, &pb.RegisterRequest{EntityId: *nombreDiscoteca, EntityType: "PRODUCER"})
	if err != nil {
		log.Fatalf("Error al registrar %s: %v", *nombreDiscoteca, err)
	}
	log.Printf("Discoteca %s registrada exitosamente.\n", *nombreDiscoteca)

	// 3. Leer el catálogo JSON asignado
	jsonFile, err := os.Open(*archivoCatalogo)
	if err != nil {
		log.Fatalf("Error abriendo el archivo %s: %v", *archivoCatalogo, err)
	}
	defer jsonFile.Close()

	byteValue, _ := io.ReadAll(jsonFile)
	var catalogo []EventoJSON
	json.Unmarshal(byteValue, &catalogo)

	// 4. Bucle infinito de envío de eventos
	log.Printf("[%s] Iniciando transmisión de eventos desde %s...\n", *nombreDiscoteca, *archivoCatalogo)
	for {
		eventoSeleccionado := catalogo[rand.Intn(len(catalogo))]

		req := &pb.Event{
			EventoId:         eventoSeleccionado.EventoID,
			Discoteca:        *nombreDiscoteca, // Forzamos el nombre de la terminal
			NombreEvento:     eventoSeleccionado.NombreEvento,
			Categoria:        eventoSeleccionado.Categoria,
			Comuna:           eventoSeleccionado.Comuna,
			Precio:           eventoSeleccionado.Precio,
			Stock:            eventoSeleccionado.Stock,
			FechaEvento:      eventoSeleccionado.FechaEvento,
			FechaPublicacion: time.Now().Format(time.RFC3339),
		}

		ctxPub, cancelPub := context.WithTimeout(context.Background(), 5*time.Second)
		res, err := client.PublishEvent(ctxPub, req)
		cancelPub()

		if err != nil {
			log.Printf("[%s] Error enviando evento: %v\n", *nombreDiscoteca, err)
		} else if res.GetAccepted() {
			log.Printf("[%s] Evento '%s' publicado.\n", *nombreDiscoteca, req.GetNombreEvento())
		} else {
			log.Printf("[%s] Evento rechazado: %s\n", *nombreDiscoteca, res.GetMessage())
		}

		tiempoEspera := time.Duration(rand.Intn(11)+30) * time.Second
		time.Sleep(tiempoEspera)
	}
}
