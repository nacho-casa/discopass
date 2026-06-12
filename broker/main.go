package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "discopass/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type server struct {
	pb.UnimplementedBrokerServiceServer
	mu         sync.Mutex
	entities   map[string]string
	dbClients  map[string]pb.DatabaseServiceClient
	bankClient pb.BankServiceClient
	events     map[string]*pb.Event // Mapa en memoria para gestionar el stock
}

// Fase 1: Registro de entidades
func (s *server) RegisterEntity(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := req.GetEntityId()
	tipo := req.GetEntityType()

	if id == "" || tipo == "" {
		return &pb.RegisterResponse{Success: false, Message: "Faltan datos en el registro"}, nil
	}

	s.entities[id] = tipo
	log.Printf("Entidad registrada: %s (Tipo: %s)\n", id, tipo)

	// Conectar a los Nodos DB
	if tipo == "DB_NODE" {
		puerto := ":50052"
		if id == "DB2" {
			puerto = ":50053"
		} else if id == "DB3" {
			puerto = ":50054"
		}
		conn, err := grpc.NewClient("localhost"+puerto, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			s.dbClients[id] = pb.NewDatabaseServiceClient(conn)
			log.Printf("Broker conectado exitosamente a %s en %s\n", id, puerto)
		}
	}

	// Conectar al Banco USM
	if tipo == "BANK" {
		conn, err := grpc.NewClient("localhost:50055", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			s.bankClient = pb.NewBankServiceClient(conn)
			log.Printf("Broker conectado exitosamente al Banco USM en :50055\n")
		}
	}

	return &pb.RegisterResponse{
		Success: true,
		Message: fmt.Sprintf("Entidad %s registrada exitosamente", id),
	}, nil
}

// Función auxiliar para replicar datos a las BDs cumpliendo W=2
func (s *server) replicateToDBs(tipo string, payload []byte) bool {
	s.mu.Lock()
	clientesDB := make(map[string]pb.DatabaseServiceClient)
	for k, v := range s.dbClients {
		clientesDB[k] = v
	}
	s.mu.Unlock()

	var acks int
	var wg sync.WaitGroup
	var mu sync.Mutex

	for nombre, cliente := range clientesDB {
		wg.Add(1)
		go func(nom string, c pb.DatabaseServiceClient) {
			defer wg.Done()
			ctxDB, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			res, err := c.WriteData(ctxDB, &pb.DBWriteRequest{Type: tipo, Payload: payload})
			if err == nil && res.GetSuccess() {
				mu.Lock()
				acks++
				mu.Unlock()
			}
		}(nombre, cliente)
	}
	wg.Wait()
	return acks >= 2 // Retorna true si se cumple W=2
}

var validCategories = map[string]bool{
	"Electrónica": true, "Reggaetón": true, "Pop": true, "Techno": true,
	"House": true, "Urbana": true, "Latina": true, "Noche Universitaria": true,
	"Fiesta Temática": true, "Retro": true, "Open Bar": true, "VIP": true,
}

// Fase 2 y 3: Recepción y Publicación de Eventos
func (s *server) PublishEvent(ctx context.Context, req *pb.Event) (*pb.PublishResponse, error) {
	if !validCategories[req.GetCategoria()] {
		return &pb.PublishResponse{Accepted: false, Message: "Categoría no válida"}, nil
	}
	if req.GetStock() <= 0 || req.GetPrecio() <= 0 {
		return &pb.PublishResponse{Accepted: false, Message: "Stock o precio inválidos"}, nil
	}

	eventoJSON, _ := json.Marshal(req)
	replicado := s.replicateToDBs("EVENTO", eventoJSON)

	if replicado {
		// Guardar en el mapa del Broker para que los usuarios puedan comprar
		s.mu.Lock()
		s.events[req.GetEventoId()] = req
		s.mu.Unlock()

		log.Printf("Evento ACEPTADO y REPLICADO: [%s] %s (Stock: %d)\n", req.GetDiscoteca(), req.GetNombreEvento(), req.GetStock())
		return &pb.PublishResponse{Accepted: true, Message: "Evento validado y replicado"}, nil
	}

	log.Printf("Evento RECHAZADO (Fallo BD): [%s] %s\n", req.GetDiscoteca(), req.GetNombreEvento())
	return &pb.PublishResponse{Accepted: false, Message: "Error de consistencia en BD"}, nil
}

// Fase 4: Consulta de Cartelera
func (s *server) GetAvailableEvents(ctx context.Context, req *pb.EmptyRequest) (*pb.EventList, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var lista []*pb.Event
	for _, ev := range s.events {
		if ev.GetStock() > 0 {
			lista = append(lista, ev)
		}
	}
	return &pb.EventList{Events: lista}, nil
}

// Fase 4: Proceso de Compra
func (s *server) BuyTicket(ctx context.Context, req *pb.BuyRequest) (*pb.BuyResponse, error) {
	s.mu.Lock()
	evento, existe := s.events[req.GetEventoId()]
	if !existe || evento.GetStock() <= 0 {
		s.mu.Unlock()
		log.Printf("Compra fallida: Evento %s no existe o sin stock.\n", req.GetEventoId())
		return &pb.BuyResponse{Success: false, Message: "Sin stock o no existe"}, nil
	}
	precio := evento.GetPrecio()
	s.mu.Unlock()

	// Validar con el Banco USM
	if s.bankClient == nil {
		log.Println("Compra fallida: Banco USM no conectado.")
		return &pb.BuyResponse{Success: false, Message: "Servicio de pago inactivo"}, nil
	}

	pagoReq := &pb.PaymentRequest{
		UsuarioId: req.GetUsuarioId(),
		Monto:     precio,
		MedioPago: req.GetMedioPago(),
	}

	pagoRes, err := s.bankClient.ProcessPayment(context.Background(), pagoReq)
	if err != nil || !pagoRes.GetApproved() {
		log.Printf("Compra rechazada para %s: Pago denegado por el Banco.\n", req.GetUsuarioId())
		return &pb.BuyResponse{Success: false, Message: "Pago rechazado"}, nil
	}

	// Si el banco aprueba, descontar stock y generar ticket
	s.mu.Lock()
	evento.Stock -= 1 // Descontamos 1 entrada
	stockActual := evento.Stock
	s.mu.Unlock()

	ticketId := fmt.Sprintf("TICKET-%s-%d", req.GetUsuarioId(), time.Now().UnixNano())

	// Replicar el ticket a las BDs
	ticketData := map[string]string{
		"ticket_id":  ticketId,
		"usuario_id": req.GetUsuarioId(),
		"evento_id":  req.GetEventoId(),
	}
	ticketJSON, _ := json.Marshal(ticketData)
	s.replicateToDBs("TICKET", ticketJSON)

	log.Printf("COMPRA EXITOSA: %s compró entrada para %s. Ticket: %s. (Stock restante: %d)\n",
		req.GetUsuarioId(), evento.GetNombreEvento(), ticketId, stockActual)

	return &pb.BuyResponse{
		Success:  true,
		TicketId: ticketId,
		Message:  "Compra confirmada",
	}, nil
}

func main() {
	port := ":50051"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Error al escuchar en el puerto %s: %v\n", port, err)
	}

	grpcServer := grpc.NewServer()
	brokerServer := &server{
		entities:  make(map[string]string),
		dbClients: make(map[string]pb.DatabaseServiceClient),
		events:    make(map[string]*pb.Event),
	}
	pb.RegisterBrokerServiceServer(grpcServer, brokerServer)

	log.Printf("Broker escuchando en el puerto %s...\n", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Error al iniciar el servidor gRPC: %v\n", err)
	}
}
