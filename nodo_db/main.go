package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
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

func (s *dbServer) WriteData(ctx context.Context, req *pb.DBWriteRequest) (*pb.DBWriteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := fmt.Sprintf("%s_%d", req.GetType(), time.Now().UnixNano())
	s.data[id] = req.GetPayload()

	log.Printf("Dato tipo %s guardado correctamente en llave: %s. (Total en BD: %d)\n", req.GetType(), id, len(s.data))
	return &pb.DBWriteResponse{Success: true}, nil
}

func (s *dbServer) SyncData(ctx context.Context, req *pb.SyncRequest) (*pb.SyncResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("⚠️ El nodo %s solicitó sincronización. Enviando %d registros...\n", req.GetRequestorId(), len(s.data))
	return &pb.SyncResponse{Data: s.data}, nil
}

func (s *dbServer) ReadEvents(ctx context.Context, req *pb.EmptyRequest) (*pb.EventList, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var lista []*pb.Event
	for key, payload := range s.data {
		if len(key) >= 7 && key[:7] == "EVENTO_" {
			var ev pb.Event
			if err := json.Unmarshal(payload, &ev); err == nil {
				lista = append(lista, &ev)
			}
		}
	}
	return &pb.EventList{Events: lista}, nil
}

func (s *dbServer) ReadHistory(ctx context.Context, req *pb.HistoryRequest) (*pb.HistoryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ticketsUsuario []*pb.TicketInfo
	for key, payload := range s.data {
		if len(key) >= 7 && key[:7] == "TICKET_" {
			var datosTicket map[string]string
			if err := json.Unmarshal(payload, &datosTicket); err == nil {
				if datosTicket["usuario_id"] == req.GetUsuarioId() {
					ticketsUsuario = append(ticketsUsuario, &pb.TicketInfo{
						TicketId: datosTicket["ticket_id"],
						EventoId: datosTicket["evento_id"],
					})
				}
			}
		}
	}
	return &pb.HistoryResponse{Tickets: ticketsUsuario}, nil
}

func (s *dbServer) recoverData(miNombre string, miPuerto string) {
	puertosConocidos := []string{":50052", ":50053", ":50054"}

	for _, puerto := range puertosConocidos {
		if puerto == miPuerto {
			continue
		}

		log.Printf("Intentando conectar con posible nodo en %s para recuperar datos...\n", puerto)
		conn, err := grpc.NewClient("localhost"+puerto, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}

		client := pb.NewDatabaseServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, err := client.SyncData(ctx, &pb.SyncRequest{RequestorId: miNombre})
		cancel()
		conn.Close()

		if err == nil && res.GetData() != nil {
			s.mu.Lock()
			for k, v := range res.GetData() {
				s.data[k] = v
			}
			cantidad := len(s.data)
			s.mu.Unlock()

			log.Printf("✅ ¡SINCRONIZACIÓN EXITOSA! Datos recuperados desde el puerto %s. Total registros: %d\n", puerto, cantidad)
			return
		}
	}
	log.Println("No se encontraron otros nodos con datos. Iniciando como base de datos en blanco.")
}

func main() {
	nombreNodo := flag.String("nombre", "DB1", "Nombre del nodo de base de datos")
	puertoLocal := flag.String("puerto", ":50052", "Puerto donde escuchará este nodo")
	flag.Parse()

	lis, err := net.Listen("tcp", *puertoLocal)
	if err != nil {
		log.Fatalf("Error al escuchar en el puerto %s: %v\n", *puertoLocal, err)
	}

	dbNode := &dbServer{
		data: make(map[string][]byte),
	}
	grpcServer := grpc.NewServer()
	pb.RegisterDatabaseServiceServer(grpcServer, dbNode)

	go func() {
		log.Printf("Nodo %s iniciado en el puerto %s...\n", *nombreNodo, *puertoLocal)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Error en el servidor DB: %v\n", err)
		}
	}()

	time.Sleep(2 * time.Second)
	dbNode.recoverData(*nombreNodo, *puertoLocal)

	brokerHost := os.Getenv("BROKER_HOST")
	if brokerHost == "" {
		brokerHost = "localhost:50051"
	}

	connBroker, err := grpc.NewClient(brokerHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("No se pudo conectar al Broker en %s: %v", brokerHost, err)
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

	select {}
}
