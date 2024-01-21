### Это простые тесты, чтоб понять возможности scylladb

Основная цель понять на сколько быстро можно записывать и считывать данные в scylladb
Возможно удастся найти оптимальные подходы для максимальной производительности.

#### Тесты проводолись на сервере с конфигурацией

CPU:	Intel Single Xeon E5-2650Lv3 12c/24t 1.8-2.5GHz
RAM:	4 x 16GB Samsung DDR4 RDIMM 2400MHz ECC
Storage:	2 x 480GB S4510 SSD Intel, 1 x 18TB HDD SAS HGST
Bandwidth:	300Mbit/s Unmetered
OS:	Ubuntu 20.04 64

```bash
scylla --version
5.4.1-0.20231231.3d22f42cf9c3
```

##### Установка ScyllaDB на сервер

[Установка ScyllaDB на Ubuntu](https://opensource.docs.scylladb.com/stable/getting-started/install-scylla/install-on-linux.html)

##### Конфигурация

```bash
nano /etc/scylla/scylla.yaml
```

[CONFIG_SCYLLA.md](CONFIG_SCYLLA.md)

##### Установка и запуск

```bash
pip3 install -r requirements.txt
```