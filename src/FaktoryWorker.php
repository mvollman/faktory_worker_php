<?php

namespace FaktoryQueue;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class FaktoryWorker {
    private $client;
    private $queues;
    private $jobTypes = [];
    private $stop = false;
    private $id = null;
    private LoggerInterface $logger;
    
    public function __construct($client, $processId = null, ?LoggerInterface $logger = null, $queues = array('default')) {
        $this->logger = $logger ?? new NullLogger();
        $this->client = $client;
        $this->queues = $queues;
        $this->id = $processId ?: substr(sha1(rand()), 0, 32);
        $this->client->setWorker($this);
    }

    public function setLogger(LoggerInterface $logger) {
        $this->logger = $logger;
    }

    public function getID() {
        return $this->id;
    }

    public function setQueues($queues) {
        $this->queues = $queues;
    }

    public function register($jobType, $callable) {
        $this->jobTypes[$jobType] = $callable;
    }

    public function run($daemonize = false) {
        do {
            $job = $this->client->fetch($this->queues);

            if ($job !== null) {
                $context = ['jid' => $job['jid'], 'jobtype' => $job['jobtype'], 'wid' => $this->id];

                if (!isset($this->jobTypes[$job['jobtype']])) {
                    $this->logger->error('No handler registered for job type', $context);
                    $this->client->fail($job['jid']);
                    continue;
                }
                $callable = $this->jobTypes[$job['jobtype']];

                try {
                    $this->logger->info('Processing job', $context);
                    call_user_func($callable, $job);
                    $this->client->ack($job['jid']);
                    $this->logger->info('Job completed', $context);
                } catch (\Exception $e) {
                    $this->logger->error('Job failed: ' . $e->getMessage(), $context + ['exception' => $e]);
                    $this->client->fail($job['jid']);
                }
            }

            usleep(100);
        } while ($daemonize && !$this->stop);
    }
}
