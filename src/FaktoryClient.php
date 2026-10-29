<?php

namespace FaktoryQueue;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class FaktoryClient {
    private $faktoryHost;
    private $faktoryPort;
    private $faktoryPassword;
    private $worker;
    private $socket = null;
    private LoggerInterface $logger;

    public function __construct($host, $port, $password = null, ?LoggerInterface $logger = null) {
        $this->logger = $logger ?? new NullLogger();
        $this->faktoryHost = $host;
        $this->faktoryPort = $port;
        $this->faktoryPassword = $password;
        $this->worker = null;
    }

    public function setLogger(LoggerInterface $logger) {
        $this->logger = $logger;
    }

    public function setWorker($worker) {
        $this->worker = $worker;
    }

    public function push($job) {
        $this->logger->debug('Pushing job', ['job' => $job]);
        $this->command('PUSH', json_encode($job));
    }

    public function fetch($queues = array('default')) {
        $this->logger->debug('Fetching job', ['queues' => $queues]);
        $response = $this->command('FETCH', implode(' ', $queues));
        if ($response[0] === '$') {
            $count = (int) trim(substr($response, 1));
            if ($count > 0) {
                $data = $this->readBytes($this->socket, $count + 2);
                return json_decode(substr($data, 0, $count), true);
            }
            return null;
        }
        return $response;
    }

    public function ack($jobId) {
        $this->logger->info('Acknowledging job', ['jid' => $jobId]);
        $this->command('ACK', json_encode(['jid' => $jobId]));
    }

    public function fail($jobId) {
        $this->logger->warning('Failing job', ['jid' => $jobId]);
        $this->command('FAIL', json_encode(['jid' => $jobId]));
    }

    /**
     * Closes the persistent connection. A new one is opened on the next command.
     */
    public function close() {
        if ($this->socket) {
            @fwrite($this->socket, "END\r\n");
            @fclose($this->socket);
        }
        $this->socket = null;
    }

    /**
     * Drops the connection without telling the server, for use in a forked child
     * that inherited the parent's socket.
     */
    public function forgetConnection() {
        $this->socket = null;
    }

    public function __destruct() {
        $this->close();
    }

    // Sends a command over the persistent connection, reconnecting once if it was dropped.
    private function command($command, $payload) {
        for ($attempt = 0; ; $attempt++) {
            if (!$this->socket) {
                $this->socket = $this->connect();
                if (!$this->socket) {
                    throw new \RuntimeException('Unable to connect to Faktory');
                }
            }
            try {
                return $this->writeLine($this->socket, $command, $payload);
            } catch (\RuntimeException $e) {
                $this->logger->warning('Faktory connection lost', ['error' => $e->getMessage()]);
                @fclose($this->socket);
                $this->socket = null;
                if ($attempt >= 1) {
                    throw $e;
                }
            }
        }
    }

    private function connect() {
        $socket = stream_socket_client("tcp://{$this->faktoryHost}:{$this->faktoryPort}", $errno, $errstr, 30);
        if (!$socket) {
            $this->logger->error("Unable to connect to Faktory: $errstr ($errno)", [
                'host' => $this->faktoryHost,
                'port' => $this->faktoryPort,
            ]);
            return false;
        } else {
            $response = $this->readLine($socket);

            $requestDefaults = [
                'v' => 2,
                'hostname' => gethostname(),
                'pid' => getmypid(),
                'labels' => ['php']
            ];

            // If the client is a worker, send the wid with request
            if ($this->worker) {
                $requestDefaults = array_merge(['wid' => $this->worker->getID()], $requestDefaults);
            }

            if (strpos($response, "\"s\":") !== false && strpos($response, "\"i\":") !== false) {
                // Requires password
                if (!$this->faktoryPassword) {
                    $this->logger->error('Faktory requires a password but none was provided');
                    throw new \Exception('Password is required.');
                }

                $payloadArray = json_decode(substr($response, strpos($response, '{')));

                $authData = $this->faktoryPassword.$payloadArray->s;
                for ($i = 0; $i < $payloadArray->i; $i++) {
                    $authData = hash('sha256', $authData, true);
                }
                
                $requestWithPassword = json_encode(array_merge(['pwdhash' => bin2hex($authData)], $requestDefaults));
                $responseWithPassword = $this->writeLine($socket, 'HELLO', $requestWithPassword);
                if (strpos($responseWithPassword, "ERR Invalid password")) {
                    $this->logger->error('Faktory password was rejected');
                    throw new \Exception('Password is incorrect.');
                }

            } else {
                // Doesn't require password
                if ($response !== "+HI {\"v\":2}\r\n") {
                    $this->logger->error('Unexpected Faktory greeting', ['response' => $response]);
                    throw new \Exception('Hi not received :(');
                }
    
                $this->writeLine($socket, 'HELLO', json_encode($requestDefaults));
            }
            $this->logger->info('Connected to Faktory', ['host' => $this->faktoryHost, 'port' => $this->faktoryPort]);
            return $socket;
        }
    }

    private function readLine($socket) {
        $contents = '';
        do {
            $chunk = fgets($socket, 1024);
            if ($chunk === false) {
                throw new \RuntimeException('Connection to Faktory closed');
            }
            $contents .= $chunk;
        } while (substr($contents, -2) !== "\r\n");
        return $contents;
    }

    private function readBytes($socket, $length) {
        $data = '';
        while (strlen($data) < $length) {
            $chunk = fread($socket, $length - strlen($data));
            if ($chunk === false || $chunk === '') {
                throw new \RuntimeException('Connection to Faktory closed');
            }
            $data .= $chunk;
        }
        return $data;
    }

    private function writeLine($socket, $command, $json) {
        $buffer = $command.' '.$json."\r\n";
        if (@fwrite($socket, $buffer) === false) {
            throw new \RuntimeException('Failed to write to Faktory');
        }
        return $this->readLine($socket);
    }
}
