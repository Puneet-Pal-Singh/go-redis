<h1 align="center">go-redis</h1>

<p align="center">
  <img alt="Github top language" src="https://img.shields.io/github/languages/top/Puneet-Pal-Singh/go-redis?color=56BEB8">
</p>

<p align="center">
  <a href="#dart-about">About</a> &#xa0; | &#xa0; 
  <a href="#sparkles-features">Features</a> &#xa0; | &#xa0;
  <a href="#rocket-technologies">Technologies</a> &#xa0; | &#xa0;
  <a href="#white_check_mark-requirements">Requirements</a> &#xa0; | &#xa0;
  <a href="#checkered_flag-starting">Starting</a> &#xa0; | &#xa0;
  <a href="#memo-license">License</a> &#xa0; | &#xa0;
  <a href="https://github.com/Puneet-Pal-Singh" target="_blank">Author</a>
</p>

<br>

## :dart: About ##

This project is a Redis-like server implemented in Go. It supports a variety of Redis commands across different data types, including Strings, Lists, Hashes, Sets, and Sorted Sets. Additionally, it provides basic server, connection, and persistence commands.


## :sparkles: Features ##

:heavy_check_mark: Available commands

- **String Commands**: SET, GET, DEL, EXISTS, INCR, DECR, INCRBY, DECRBY, MSET, MGET
- **List Commands**: LPUSH, RPUSH, LPOP, RPOP, LLEN
- **Hash Commands**: HSET, HGET, HDEL, HLEN, HMGET, HGETALL
- **Set Commands**: SADD, SREM, SMEMBERS, SISMEMBER
- **Sorted Set Commands**: ZADD, ZRANGE, ZREM
- **Server and Connection Commands**: EXPIRE, TTL, INFO, FLUSHALL, PING
- **Persistence Commands**: SAVE, BGSAVE

:heavy_check_mark: Persistence commands Saves data to disk and loads it on startup.

:heavy_check_mark: publish/subscribe functionality for real-time messaging.


## :rocket: References ##


- [Redis Documentation - Data Types](https://redis.io/docs/latest/develop/data-types/)
- [Redis Best Practices](https://www.dragonflydb.io/guides/redis-best-practices)
- [Build Redis from Scratch](https://www.build-redis-from-scratch.dev/en/introduction)
- [Redis Persistence Deep Dive](https://www.memurai.com/blog/redis-persistence-deep-dive)
- [Redis Persistence Guide](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Redis Import Guide](https://redis.io/learn/guides/import)


## :white_check_mark: Requirements ##

Before starting :checkered_flag:, you need to have [Git](https://git-scm.com) and [Go](https://go.dev/) installed or Docker (optional, for containerization).

## :checkered_flag: Installation ##


1. Clone the repository:

   ```bash
   git clone https://github.com/Puneet-Pal-Singh/go-redis.git
   cd go-redis
   ```

2. Install dependencies:

   ```bash
   go mod download
   ```

3. Build the application:

   ```bash
   go build -o go-redis
   ```

### Running the Application

You can run the application directly or use Docker.

#### Directly

  ```bash
   go run ./main
  ```

#### Using `Docker`

1. Build the Docker image:

   ```bash
   docker build -t go-redis .
   ```

2. Run the Docker container:

   ```bash
   docker run -p 6378:6378 go-redis
   ```

### Usage

Once the server is running, you can connect to it using a Redis client or through a terminal. The server listens on port `6378`.

#### Example Commands

- Set a value:

  ```bash
  SET key value
  ```

- Get a value:

  ```bash
  GET key
  ```

- Delete a key:

  ```bash
  DEL key
  ```
### Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or features.

## :memo: License ##

This project is under license from MIT. For more details, see the [LICENSE](LICENSE.md) file.


Made with :heart: by <a href="https://github.com/Puneet-Pal-Singh" target="_blank">Puneet Pal Singh</a>

&#xa0;

<a href="#top">Back to top</a>
