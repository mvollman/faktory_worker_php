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

    /**
     * @param bool $daemonize    Keep running until stopped.
     * @param int  $concurrency  Number of jobs processed in parallel. Values above 1
     *                           fork that many child processes (requires ext-pcntl),
     *                           each with its own Faktory connection and worker ID.
     */
    public function run($daemonize = false, $concurrency = 1) {
        if ($concurrency <= 1) {
            $this->loop($daemonize);
            return;
        }
        if (!function_exists('pcntl_fork')) {
            throw new \RuntimeException('Concurrency requires the pcntl extension');
        }
        $this->runConcurrent($daemonize, (int) $concurrency);
    }

    private function runConcurrent($daemonize, $concurrency) {
        $children = [];
        $baseId = $this->id;
        $stopping = false;

        pcntl_async_signals(true);
        $handler = function ($signo) use (&$children, &$stopping) {
            $stopping = true;
            foreach (array_keys($children) as $pid) {
                posix_kill($pid, $signo);
            }
        };
        pcntl_signal(SIGTERM, $handler);
        pcntl_signal(SIGINT, $handler);

        $spawn = function ($slot) use (&$children, $baseId, $daemonize) {
            $pid = pcntl_fork();
            if ($pid === -1) {
                throw new \RuntimeException('Unable to fork worker process');
            }
            if ($pid === 0) {
                pcntl_signal(SIGTERM, function () { $this->stop = true; });
                pcntl_signal(SIGINT, function () { $this->stop = true; });
                $this->id = $baseId . '-' . $slot;
                $this->client->forgetConnection();
                try {
                    $this->loop($daemonize);
                } catch (\Throwable $e) {
                    $this->logger->error('Worker process crashed: ' . $e->getMessage(), ['exception' => $e]);
                    exit(1);
                }
                exit(0);
            }
            $children[$pid] = $slot;
        };

        // The parent must not keep a connection that children would share.
        $this->client->forgetConnection();
        for ($slot = 0; $slot < $concurrency; $slot++) {
            $spawn($slot);
        }

        while ($children) {
            $pid = pcntl_wait($status);
            if ($pid <= 0) {
                if ($pid === -1 && pcntl_get_last_error() === PCNTL_ECHILD) {
                    break;
                }
                continue;
            }
            $slot = $children[$pid] ?? null;
            unset($children[$pid]);
            if ($daemonize && !$stopping && $slot !== null) {
                $this->logger->warning('Worker process exited, restarting', ['pid' => $pid, 'slot' => $slot]);
                sleep(1);
                $spawn($slot);
            }
        }
    }

    private function loop($daemonize) {
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
