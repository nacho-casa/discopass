package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "discopass/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type dbServer struct {
	pb.UnimplementedDatabaseServiceServer
	mu   sync.Mutex
	data map[string][]byte
}

// WriteData: Recibe datos del Broker y los guarda
func (s *dbServer) WriteData(ctx context.Context, req *pb.DBWriteRequest) (*pb.DBWriteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Generamos un ID único basado en el tiempo exacto para no sobreescribir
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	s.data[id] = req.GetPayload()

	log.Printf("Dato tipo %s guardado correctamente. (Total en BD: %d registros)\n", req.GetType(), len(s.data))
	return &pb.DBWriteResponse{Success: true}, nil
}

// SyncData: Otro nodo que se acaba de prender nos pide los datos
func (s *dbServer) SyncData(ctx context.Context, req *pb.SyncRequest) (*pb.SyncResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("⚠️ El nodo %s solicitó sincronización. Enviando %d registros...\n", req.GetRequestorId(), len(s.data))

	// Le devolvemos una copia exacta de nuestro mapa
	return &pb.SyncResponse{Data: s.data}, nil
}

// recoverData: Lógica para pedir datos a otros nodos al encenderse
func (s *dbServer) recoverData(miNombre string, miPuerto string) {
	puertosConocidos := []string{":50052", ":50053", ":50054"}

	for _, puerto := range puertosConocidos {
		// No nos vamos a pedir datos a nosotros mismos
		if puerto == miPuerto {
			continue
		}

		log.Printf("Intentando conectar con posible nodo en %s para recuperar datos...\n", puerto)
		conn, err := grpc.NewClient("localhost"+puerto, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue // Si no conecta, probamos con el siguiente
		}

		client := pb.NewDatabaseServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, err := client.SyncData(ctx, &pb.SyncRequest{RequestorId: miNombre})
		cancel()
		conn.Close()

		// Si el nodo respondió exitosamente y nos mandó datos
		if err == nil && res.GetData() != nil {
			s.mu.Lock()
			for k, v := range res.GetData() {
				s.data[k] = v // Copiamos el historial a nuestra memoria local
			}
			cantidad := len(s.data)
			s.mu.Unlock()

			log.Printf("✅ ¡SINCRONIZACIÓN EXITOSA! Datos recuperados desde el puerto %s. Total registros: %d\n", puerto, cantidad)
			return // Con resincronizarnos con 1 solo nodo es suficiente
		}
	}

	log.Println("No se encontraron otros nodos con datos. Iniciando como base de datos en blanco.")
}

func main() {
	nombreNodo := flag.String("nombre", "DB1", "Nombre del nodo de base de datos")
	puertoLocal := flag.String("puerto", ":50052", "Puerto donde escuchará este nodo")
	flag.Parse()

	// 1. Iniciar servidor gRPC
	lis, err := net.Listen("tcp", *puertoLocal)
	if err != nil {
		log.Fatalf("Error al escuchar en el puerto %s: %v\n", *puertoLocal, err)
	}

	dbNode := &dbServer{
		data: make(map[string][]byte),
	}
	grpcServer := grpc.NewServer()
	pb.RegisterDatabaseServiceServer(grpcServer, dbNode)

	// Ejecutar el servidor para escuchar peticiones en segundo plano
	go func() {
		log.Printf("Nodo %s iniciado en el puerto %s...\n", *nombreNodo, *puertoLocal)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Error en el servidor DB: %v\n", err)
		}
	}()

	// 2. FASE 5: Intentar recuperar datos antes de avisarle al Broker
	// Damos 2 segundos de gracia para asegurar que el servidor gRPC local levantó
	time.Sleep(2 * time.Second)
	dbNode.recoverData(*nombreNodo, *puertoLocal)

	// 3. FASE 1: Registrarse en el Broker Central
	connBroker, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("No se pudo conectar al Broker: %v", err)
	}
	defer connBroker.Close()

	brokerClient := pb.NewBrokerServiceClient(connBroker)
	_, err = brokerClient.RegisterEntity(context.Background(), &pb.RegisterRequest{
		EntityId:   *nombreNodo,
		EntityType: "DB_NODE",
	})
	if err != nil {
		log.Fatalf("Error al registrar el nodo en el Broker: %v", err)
	}
	log.Printf("Nodo %s registrado oficialmente en el Broker y listo para operar.\n", *nombreNodo)

	// Mantener vivo el programa
	select {}
}
