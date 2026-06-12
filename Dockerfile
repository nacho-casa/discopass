# Usamos Go 1.25 basado en Alpine Linux para que la imagen sea muy ligera
FROM golang:1.25-alpine

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiamos los archivos de dependencias primero
COPY go.mod go.sum ./

# Descargamos las dependencias
RUN go mod download

# Copiamos todo el código fuente al contenedor
COPY . .

# El comando de ejecución se le pasará dinámicamente desde el docker-compose