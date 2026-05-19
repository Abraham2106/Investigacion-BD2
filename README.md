# Investigacion de Bases de Datos II 

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Apache Ignite](https://img.shields.io/badge/Apache_Ignite-E47911?style=for-the-badge&logo=apache&logoColor=white)](https://ignite.apache.org)
[![Apache Kudu](https://img.shields.io/badge/Apache_Kudu-1E90FF?style=for-the-badge&logo=apache&logoColor=white)](https://kudu.apache.org)
[![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)](https://grafana.com)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker&logoColor=white)](./docker-compose.yml)

<br/>
Sistema de E-Commerce basado en FastAPI + Apache Kafka + Apache Ignite + PySpark + Apache Kudu, diseñado para ingerir, procesar y almacenar pedidos en tiempo real, detectando fraudes y filtrando zonas de riesgo.

## 🎯 Objetivo de Investigación 

Ofrecer una explicación clara y detallada de los motores de datos Apache Ignite, Apache Kafka y Apache Kudu. Para cada uno de los motores de base de datos se va a analizar su arquitectura interna, modelos de consistencia, mecanismos de despliegue y casos de uso, para evaluar su importancia en proyectos modernos de ingeniería de datos que demandan bajas latencias y manejo de volúmenes masivos.

---

## 🏡 Arquitectura
[![Arquitectura](https://img.shields.io/badge/Arquitectura-del%20Sistema-6A5ACD?style=for-the-badge&logo=markdown&logoColor=white)](https://github.com/Abraham2106/Investigacion-BD2/blob/main/E-Commerce%20Triad/Docs/arquitectura-del-sistema.md)

Este proyecto hace uso de Apache Kafka, Apache Ignite, PySpark y Apache Kudu para crear un sistema de E-Commerce. Los datos ingestados serán producidos por un script de Python que simula la actividad de un sitio de E-Commerce, generando datos de ventas, productos, zonas, etc. Este sistema debe poder clasificar los datos por zonas y precio para así poder filtrar y banear los pedidos de zonas inseguras y con precios sospechosos.

---

## 📄 Requerimientos

[![Requerimientos](https://img.shields.io/badge/Requerimientos-del%20Sistema-6A5ACD?style=for-the-badge&logo=markdown&logoColor=white)](https://github.com/Abraham2106/Investigacion-BD2/blob/main/E-Commerce%20Triad/Docs/especificacion-de-requerimientos.md)

---
## Documentacion

[![Documentación](https://img.shields.io/badge/Ver-Documentación-4285F4?style=for-the-badge&logo=googledocs&logoColor=white)](https://docs.google.com/document/d/10PWoX6MbLVNgIVu5Lw9Z3764G7nASzMNs2OlJxMfQqk/edit?usp=sharing)

<img width="626" height="806" alt="image" src="https://github.com/user-attachments/assets/5430ed5f-c331-412f-9e44-0d21af7e48a3" />

---

## 👥 Equipo 

- Abraham Gerardo Solano Parrales
- Sofia Elena Barrantes Miranda
- Daniel Josué Herrera Córdoba
- Kevin David Jiménez Escalante

---

<div align="center">

Construido con Apache Kafka &nbsp;·&nbsp; Apache Ignite &nbsp;·&nbsp; Apache Kudu &nbsp;·&nbsp; Grafana &nbsp;·&nbsp; Python

</div>
