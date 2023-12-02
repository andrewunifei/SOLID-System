package main

import (
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/andrewunifei/SOLID-system/internal/infra/akafka"
	"github.com/andrewunifei/SOLID-system/internal/infra/repository"
	"github.com/andrewunifei/SOLID-system/internal/infra/web"
	"github.com/andrewunifei/SOLID-system/internal/usecase"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-chi/chi/v5"
)

func main() {
	db, err := sql.Open("mysql", "root:root@tcp(host.docker.internal:3306/products)")

	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Repositório. Adaptador MySQL nesse caso em particular,
	// mas estamos abstraindo a aplicação para aceitar qualquer repositório (SOLID)
	repository := repository.NewProductRepositoryMysql(db)
	createProductUseCase := usecase.NewCreateProductUseCase(repository)
	listProductUseCase := usecase.NewListProductsUseCase(repository)

	// HTTP
	productHandlers := web.NewProductHandlers(createProductUseCase, listProductUseCase)

	r := chi.NewRouter()
	r.Post("/products", productHandlers.CreateProductHandler)
	r.Get("/products", productHandlers.ListProductsHandler)

	// Thread do servidor
	go http.ListenAndServe(":8000", r)

	// Kafka
	msgChan := make(chan *kafka.Message)
	go akafka.Consume([]string{"products"}, "host.docker.internal:9094", msgChan)

	for msg := range msgChan {
		dto := usecase.CreateProductInputDto{}
		err := json.Unmarshal(msg.Value, &dto)

		if err != nil {
			continue
		}

		_, err = createProductUseCase.Execute(dto)

		if err != nil {
			continue
		}
	}
}