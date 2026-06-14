package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"net"
	"os"
	"time"

	pb "discopass/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type bankServer struct {
	pb.UnimplementedBankServiceServer
}

func (s *bankServer) ProcessPayment(ctx context.Context, req *pb.PaymentRequest) (*pb.PaymentResponse, error) {
	// Refrescar la semilla en cada llamada para mayor aleatoriedad
	rand.Seed(time.Now().UnixNano())

	probabilidad := 80
	if req.GetMedioPago() == "credito" {
		probabilidad = 90
	}

	aprobado := rand.Intn(100) < probabilidad

	resultado := "Rechazado"
	if aprobado {
		resultado = "Aprobado"
	}

	log.Printf("Operación: Usuario=%s | Monto=$%d | Medio=%s | Resultado: %s\n",
		req.GetUsuarioId(), req.GetMonto(), req.GetMedioPago(), resultado)

	return &pb.PaymentResponse{Approved: aprobado}, nil
}

func main() {
	puerto := flag.String("puerto", ":50055", "Puerto del Banco USM")
	flag.Parse()

	lis, err := net.Listen("tcp", *puerto)
	if err != nil {
		log.Fatalf("Error al escuchar en %s: %v", *puerto, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBankServiceServer(grpcServer, &bankServer{})

	go func() {
		log.Printf("Banco USM escuchando en el puerto %s...\n", *puerto)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Error en servidor Banco: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)

	// Leer IP del Broker desde las variables de entorno
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.RegisterEntity(ctx, &pb.RegisterRequest{
		EntityId:   "Banco USM",
		EntityType: "BANK",
	})

	if err != nil {
		log.Fatalf("Error al registrar el Banco en el Broker: %v", err)
	}
	log.Println("Banco USM registrado exitosamente en el Broker.")

	// Mantiene el contenedor vivo escuchando peticiones
	select {}
}
