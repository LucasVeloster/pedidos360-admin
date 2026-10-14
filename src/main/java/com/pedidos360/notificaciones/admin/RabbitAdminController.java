package com.pedidos360.notificaciones.admin;

import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.core.Queue;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/admin")
public class RabbitAdminController {

    private final RabbitAdmin rabbitAdmin;

    public RabbitAdminController(
            org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory) {
        this.rabbitAdmin = new RabbitAdmin(connectionFactory);
    }

    @PostMapping("/colas/{nombre}")
    public String crearCola(@PathVariable String nombre) {
        if (nombre.isBlank()) {
            throw new IllegalArgumentException("Nombre obligatorio");
        }
        rabbitAdmin.declareQueue(new Queue(nombre, true));
        return "Cola creada: " + nombre;
    }

    @DeleteMapping("/colas/{nombre}")
    public String eliminarCola(@PathVariable String nombre) {
        rabbitAdmin.deleteQueue(nombre);
        return "Solicitud de eliminación de cola: " + nombre;
    }

    @PostMapping("/exchanges/{nombre}")
    public String crearExchange(@PathVariable String nombre) {
        rabbitAdmin.declareExchange(
            new org.springframework.amqp.core.DirectExchange(nombre, true, false)
        );
        return "Exchange creado: " + nombre;
    }


    @DeleteMapping("/exchanges/{nombre}")
    public String eliminarExchange(@PathVariable String nombre) {
        rabbitAdmin.deleteExchange(nombre);
        return "Solicitud de eliminación de exchange: " + nombre;
    }


    @PostMapping("/bindings")
    public String crearBinding(
            @RequestParam String cola,
            @RequestParam String exchange,
            @RequestParam String routingKey) {

        if (cola.isBlank() || exchange.isBlank() || routingKey.isBlank()) {
            throw new IllegalArgumentException("Parametros obligatorios");
        }

        rabbitAdmin.declareBinding(
            new org.springframework.amqp.core.Binding(
                cola,
                org.springframework.amqp.core.Binding.DestinationType.QUEUE,
                exchange,
                routingKey,
                null
            )
        );

        return "Binding creado correctamente";
    }


    @DeleteMapping("/bindings")
    public String eliminarBinding(
            @RequestParam String cola,
            @RequestParam String exchange,
            @RequestParam String routingKey) {

        rabbitAdmin.removeBinding(
            new org.springframework.amqp.core.Binding(
                cola,
                org.springframework.amqp.core.Binding.DestinationType.QUEUE,
                exchange,
                routingKey,
                null
            )
        );

        return "Binding eliminado correctamente";
    }

}
