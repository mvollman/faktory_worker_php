<?php

namespace FaktoryQueue;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class FaktoryClient {
    private $faktoryHost;
    private $faktoryPort;
    private $faktoryPassword;
    private $worker;
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
        $socket = $this->connect();
        $this->logger->debug('Pushing job', ['job' => $job]);
        $this->writeLine($socket, 'PUSH', json_encode($job));
        $this->close($socket);
    }

    public function fetch($queues = array('default')) {
        $socket = $this->connect();
        $this->logger->debug('Fetching job', ['queues' => $queues]);
        $response = $this->writeLine($socket, 'FETCH', implode(' ', $queues));
        $char = $response[0];
        if ($char === '$') {
            $count = trim(substr($response, 1, strpos($response, "\r\n")));
            $data = null;
            if ($count > 0) {
                $data = $this->readLine($socket);
                $this->close($socket);
                return json_decode($data, true);
            }
            return $data;
        }
        $this->close($socket);
        return $response;
    }

    public function ack($jobId) {
        $socket = $this->connect();
        $this->logger->info('Acknowledging job', ['jid' => $jobId]);
        $this->writeLine($socket, 'ACK', json_encode(['jid' => $jobId]));
        $this->close($socket);
    }

    public function fail($jobId) {
        $socket = $this->connect();
        $this->logger->warning('Failing job', ['jid' => $jobId]);
        $this->writeLine($socket, 'FAIL', json_encode(['jid' => $jobId]));
        $this->close($socket);
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
        $contents = fgets($socket, 1024);
        while (strpos($contents, "\r\n") === false) {
            $contents .= fgets($socket, 1024 - strlen($contents));
        }
        return $contents;
    }

    private function writeLine($socket, $command, $json) {
        $buffer = $command.' '.$json."\r\n";
        stream_socket_sendto($socket, $buffer);
        $read = $this->readLine($socket);
        return $read;
    }

    private function close($socket) {
        fclose($socket);
    }
}
