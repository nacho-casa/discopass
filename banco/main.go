package main

import (
	"context"
	"log"
	"math/rand"
	"net"
	"time"

	pb "discopass/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type bankServer struct {
	pb.UnimplementedBankServiceServer
}

// ProcessPayment implementa la lógica estocástica del Banco USM
func (s *bankServer) ProcessPayment(ctx context.Context, req *pb.PaymentRequest) (*pb.PaymentResponse, error) {
	// Generador de números aleatorios
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	probabilidadAprobacion := 80
	if req.GetMedioPago() == "credito" {
		probabilidadAprobacion = 90
	}

	// rng.Intn(100) genera un número entre 0 y 99
	aprobado := rng.Intn(100) < probabilidadAprobacion

	resultadoLog := "Rechazado (Fondos insuficientes)"
	if aprobado {
		resultadoLog = "Aprobado"
	}

	// Registro detallado exigido por el laboratorio
	log.Printf("Operación: Usuario=%s | Monto=$%d | Medio=%s | Resultado: %s\n",
		req.GetUsuarioId(), req.GetMonto(), req.GetMedioPago(), resultadoLog)

	return &pb.PaymentResponse{Approved: aprobado}, nil
}

func main() {
	// Puerto asignado para el Banco
	puertoLocal := ":50055"
	lis, err := net.Listen("tcp", puertoLocal)
	if err != nil {
		log.Fatalf("Error al escuchar en el puerto %s: %v\n", puertoLocal, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBankServiceServer(grpcServer, &bankServer{})

	// Ejecutar servidor en segundo plano
	go func() {
		log.Printf("Banco USM escuchando en el puerto %s...\n", puertoLocal)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Error en el servidor del Banco: %v\n", err)
		}
	}()

	// Registrar el Banco en el Broker Central
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("No se pudo conectar al Broker: %v", err)
	}
	defer conn.Close()

	client := pb.NewBrokerServiceClient(conn)
	_, err = client.RegisterEntity(context.Background(), &pb.RegisterRequest{
		EntityId:   "Banco USM",
		EntityType: "BANK",
	})
	if err != nil {
		log.Fatalf("Error al registrar el Banco: %v", err)
	}
	log.Println("Banco USM registrado exitosamente en el Broker.")

	// Mantener el proceso vivo
	select {}
}
