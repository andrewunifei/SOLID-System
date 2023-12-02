package main

import (
	"database/sql"
	"encoding/json"
	"github.com/andrewunifei/SOLID-system/internal/infra/akafka"
	"github.com/andrewunifei/SOLID-system/internal/infra/repository"
	"github.com/andrewunifei/SOLID-system/internal/usecase"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	db, err := sql.Open("mysql", "root:root@tcp(host.docker.internal:3306/products)")

	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Para comunicação entre threads
	msgChan := make(chan *kafka.Message)
	go akafka.Consume([]string{"products"}, "host.docker.internal:9094", msgChan)

	// Repositório. Adaptador MySQL nesse caso em particular,
	// mas estamos abstraindo a aplicação para aceitar qualquer repositório (SOLID)
	repository := repository.NewProductRepositoryMysql(db)
	createProductUseCase := usecase.NewCreateProductUseCase(repository)

	for msg := range msgChan {
		dto := usecase.CreateProductInputDto{}
		err := json.Unmarshal(msg.Value, &dto)

		if err != nil {
			continue
		}

		_, err = createProductUseCase.Execute(dto)
	}


	listProductsUseCase := usecase.NewListProductsUseCase(repository)
}