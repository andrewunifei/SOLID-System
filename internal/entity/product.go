package entity

import "github.com/google/uuid"

type Product struct {
	ID string
	Name string
	Price float64
}

type ProductRepository interface {
	Create(product *Product) error
	FindAll() ([]*Product, error)
}
 
func NewProduct(name string, price float64) *Product {
	return &Product{
		ID: uuid.New().String(),
		Name: name,
		Price: price,
	}
}