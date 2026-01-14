curl -X DELETE http://localhost:8083/connectors/project-orders-cdc && \
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
    http://localhost:8083/connectors/ -d @register.ecommerce.orders.json

