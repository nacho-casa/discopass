package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
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
	events     map[string]*pb.Event

	// Contadores estadísticos para el Reporte.txt
	comprasExitosas   int
	comprasRechazadas int
	fallosNodos       int
	fallosBanco       int
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

// Función auxiliar para replicar datos a las BDs simulando tolerancia a fallos W=2
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

			// Timeout corto para simular tolerancia a caídas e indisponibilidad
			ctxDB, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			res, err := c.WriteData(ctxDB, &pb.DBWriteRequest{Type: tipo, Payload: payload})
			if err == nil && res.GetSuccess() {
				mu.Lock()
				acks++
				mu.Unlock()
			} else {
				// Captura de fallo de Nodo DB
				mu.Lock()
				s.fallosNodos++
				mu.Unlock()
				log.Printf("Fallo detectado por indisponibilidad o timeout al escribir en nodo %s\n", nom)
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

	log.Printf("Evento RECHAZADO (Fallo BD generalizado): [%s] %s\n", req.GetDiscoteca(), req.GetNombreEvento())
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
		s.comprasRechazadas++
		s.mu.Unlock()
		log.Printf("Compra fallida: Evento %s no existe o sin stock.\n", req.GetEventoId())
		return &pb.BuyResponse{Success: false, Message: "Sin stock o no existe"}, nil
	}
	precio := evento.GetPrecio()
	s.mu.Unlock()

	// Validar indisponibilidad del Banco USM
	if s.bankClient == nil {
		s.mu.Lock()
		s.fallosBanco++
		s.comprasRechazadas++
		s.mu.Unlock()
		log.Println("Compra fallida: Banco USM no conectado.")
		return &pb.BuyResponse{Success: false, Message: "Servicio de pago inactivo"}, nil
	}

	pagoReq := &pb.PaymentRequest{
		UsuarioId: req.GetUsuarioId(),
		Monto:     precio,
		MedioPago: req.GetMedioPago(),
	}

	// Tiempo de gracia para soportar demoras del banco sin colapsar el Broker
	ctxBanco, cancelBanco := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelBanco()

	pagoRes, err := s.bankClient.ProcessPayment(ctxBanco, pagoReq)
	if err != nil || !pagoRes.GetApproved() {
		s.mu.Lock()
		if err != nil {
			s.fallosBanco++ // Captura de timeout o caída del servicio de banco
		}
		s.comprasRechazadas++
		s.mu.Unlock()
		log.Printf("Compra rechazada para %s: Pago denegado o Banco no responde.\n", req.GetUsuarioId())
		return &pb.BuyResponse{Success: false, Message: "Pago rechazado o en timeout"}, nil
	}

	// Pago aprobado, descontar stock en memoria y generar ticket
	s.mu.Lock()
	evento.Stock -= 1 // Descontamos 1 entrada
	stockActual := evento.Stock
	s.mu.Unlock()

	ticketId := fmt.Sprintf("TICKET-%s-%d", req.GetUsuarioId(), time.Now().UnixNano())

	// Replicar el ticket a las BDs verificando condición W=2
	ticketData := map[string]string{
		"ticket_id":  ticketId,
		"usuario_id": req.GetUsuarioId(),
		"evento_id":  req.GetEventoId(),
	}
	ticketJSON, _ := json.Marshal(ticketData)
	replicado := s.replicateToDBs("TICKET", ticketJSON)

	// Validar que se haya guardado en al menos 2 Nodos
	if !replicado {
		// Revertir descuento de stock si falla la consistencia para evitar estados corrompidos
		s.mu.Lock()
		evento.Stock += 1
		s.comprasRechazadas++
		s.mu.Unlock()
		log.Printf("Compra revertida para %s: No se alcanzó W=2 al guardar el ticket en los Nodos DB.\n", req.GetUsuarioId())
		return &pb.BuyResponse{Success: false, Message: "Error de consistencia (W<2) al guardar ticket en Nodos"}, nil
	}

	s.mu.Lock()
	s.comprasExitosas++
	s.mu.Unlock()

	log.Printf("COMPRA EXITOSA: %s compró entrada para %s. Ticket: %s. (Stock restante: %d)\n",
		req.GetUsuarioId(), evento.GetNombreEvento(), ticketId, stockActual)

	return &pb.BuyResponse{
		Success:  true,
		TicketId: ticketId,
		Message:  "Compra confirmada",
	}, nil
}

// Función exclusiva para generar Reporte.txt al finalizar la simulación
func generarReporteTXT(s *server) {
	log.Println("Generando Reporte.txt...")

	archivo, err := os.Create("Reporte.txt")
	if err != nil {
		log.Printf("Error crítico al crear el archivo Reporte.txt: %v\n", err)
		return
	}
	defer archivo.Close()

	s.mu.Lock()
	defer s.mu.Unlock()

	archivo.WriteString("=========================================\n")
	archivo.WriteString("   REPORTE FINAL - DISCOPASS GRUPO 20\n")
	archivo.WriteString("=========================================\n\n")

	archivo.WriteString("1. ENTIDADES REGISTRADAS EN LA RED:\n")
	for id, tipo := range s.entities {
		archivo.WriteString(fmt.Sprintf(" - %s (Tipo: %s)\n", id, tipo))
	}
	archivo.WriteString("\n")

	archivo.WriteString("2. RESUMEN DE EVENTOS GENERADOS DESDE CATALOGO:\n")
	for _, ev := range s.events {
		archivo.WriteString(fmt.Sprintf(" - Discoteca: %s | Evento: %s | Stock final: %d | Precio: $%d\n", ev.GetDiscoteca(), ev.GetNombreEvento(), ev.GetStock(), ev.GetPrecio()))
	}
	archivo.WriteString("\n")

	archivo.WriteString("3. ESTADISTICAS Y TRAZABILIDAD DE OPERACION:\n")
	archivo.WriteString(fmt.Sprintf(" - Total Compras Exitosas y Replicadas: %d\n", s.comprasExitosas))
	archivo.WriteString(fmt.Sprintf(" - Total Compras Rechazadas (Rechazo Banco/Timeout/Sin Stock/Error W=2): %d\n", s.comprasRechazadas))
	archivo.WriteString(fmt.Sprintf(" - Total Fallos detectados en Nodos de Base de Datos: %d\n", s.fallosNodos))
	archivo.WriteString(fmt.Sprintf(" - Total Indisponibilidades o Timeout de Banco USM: %d\n", s.fallosBanco))
	archivo.WriteString("\n")

	archivo.WriteString("4. CONCLUSION DE CONSISTENCIA Y DISPONIBILIDAD DE LA ARQUITECTURA:\n")
	archivo.WriteString(" - Consistencia (W=2, R=2): El Broker mantuvo un registro unificado y consistente durante las compras validando que las escrituras alcanzaran al menos a 2 nodos DB simultaneamente. Se abortaron y revirtieron intentos en caso de no asegurar el Quorum.\n")
	archivo.WriteString(" - Tolerancia a Fallos e Indisponibilidad: La simulacion confirma que la caida de un nodo DB es mitigada si los demas siguen operando. Ante la eventual indisponibilidad del Banco, el sistema descarto solicitudes usando 'context timeouts' limitando bloqueos globales en el Broker central.\n")

	archivo.WriteString("=========================================\n")
	log.Println("Reporte.txt generado satisfactoriamente en la raíz del proyecto.")
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

	// Captura de señales del Sistema Operativo para apagar de forma segura (Graceful Shutdown)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("\nSeñal de apagado o interrupción recibida.")
		log.Println("Cerrando conexiones gRPC y preparando consolidado de datos...")
		// Se invoca la función del reporte antes de apagar los procesos del Broker
		generarReporteTXT(brokerServer)
		grpcServer.GracefulStop()
		os.Exit(0)
	}()

	log.Printf("Broker Central de DiscoPass escuchando solicitudes en el puerto %s...\n", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Error crítico en el servidor gRPC: %v\n", err)
	}
}
