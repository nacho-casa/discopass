package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
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
	nombreDiscoteca := flag.String("nombre", "DataClub", "Nombre de la discoteca productora")
	archivoCatalogo := flag.String("catalogo", "catalogo_eventos_30.json", "Archivo JSON a leer")
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
	_, err = client.RegisterEntity(ctxReg, &pb.RegisterRequest{EntityId: *nombreDiscoteca, EntityType: "PRODUCER"})
	if err != nil {
		log.Fatalf("Error al registrar %s: %v", *nombreDiscoteca, err)
	}
	log.Printf("Discoteca %s registrada exitosamente.\n", *nombreDiscoteca)

	jsonFile, err := os.Open(*archivoCatalogo)
	if err != nil {
		log.Fatalf("Error abriendo el archivo %s: %v", *archivoCatalogo, err)
	}
	defer jsonFile.Close()

	byteValue, _ := io.ReadAll(jsonFile)
	var catalogoCompleto []EventoJSON
	json.Unmarshal(byteValue, &catalogoCompleto)

	// =====================================================================
	// NUEVA LÓGICA DE FILTRADO
	// =====================================================================
	var miCatalogo []EventoJSON
	for _, ev := range catalogoCompleto {
		// Solo guardamos los eventos que pertenezcan a esta discoteca en particular
		if ev.Discoteca == *nombreDiscoteca {
			miCatalogo = append(miCatalogo, ev)
		}
	}

	// Validamos que el archivo JSON realmente contenga eventos para esta discoteca
	if len(miCatalogo) == 0 {
		log.Fatalf("[%s] ERROR CRÍTICO: No se encontró ningún evento para esta discoteca en el archivo %s", *nombreDiscoteca, *archivoCatalogo)
	}

	log.Printf("[%s] Catálogo filtrado exitosamente: Se encontraron %d eventos propios en %s.\n", *nombreDiscoteca, len(miCatalogo), *archivoCatalogo)
	log.Printf("[%s] Iniciando transmisión continua...\n", *nombreDiscoteca)

	log.Printf("[%s] Esperando 15 segundos para que los Nodos DB alcancen el Quórum...\n", *nombreDiscoteca)
	time.Sleep(15 * time.Second)

	log.Printf("[%s] Iniciando transmisión continua...\n", *nombreDiscoteca)
	// Bucle infinito para mantener la emisión continua exigida en la pauta
	for {
		// Iteramos ÚNICAMENTE sobre el catálogo filtrado
		for _, eventoSeleccionado := range miCatalogo {
			nuevoEventoID := fmt.Sprintf("%s-%d", eventoSeleccionado.EventoID, time.Now().UnixNano())

			req := &pb.Event{
				EventoId:         nuevoEventoID,
				Discoteca:        eventoSeleccionado.Discoteca, // Ahora respeta la discoteca original (que es la misma del -nombre)
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
				log.Printf("[%s] Evento '%s' publicado (ID dinámico: %s).\n", *nombreDiscoteca, req.GetNombreEvento(), nuevoEventoID)
			} else {
				log.Printf("[%s] Evento rechazado: %s\n", *nombreDiscoteca, res.GetMessage())
			}

			// Pausa aleatoria de 30 a 40 segundos exigida
			tiempoEspera := time.Duration(rand.Intn(11)+30) * time.Second
			log.Printf("[%s] Esperando %v antes de publicar el siguiente evento...\n", *nombreDiscoteca, tiempoEspera)
			time.Sleep(tiempoEspera)
		}

		log.Printf("[%s] Se han publicado todos los eventos propios. Reiniciando ciclo...\n", *nombreDiscoteca)
	}
}
