<?php
require_once dirname(__FILE__) . DIRECTORY_SEPARATOR . 'bootstrap.php';


class Test_Job_Timeout {

    public $args = [];
    public function getJobQueueId(){
        return 1;
    }

    public function perform(){
        if(!empty($this->args['sleep'])){
            sleep($this->args['sleep']);
        }
        return true;
    }
    public function timeoutLimit()
    {
        return 1;
    }
}

/**
 * Resque_Worker tests.
 *
 * @package     Resque/Tests
 * @author      Chris Boulton <chris@bigcommerce.com>
 * @license     http://www.opensource.org/licenses/mit-license.php
 */
class Resque_Tests_WorkerTest extends Resque_Tests_TestCase
{
    public function testWorkerRegistersInList()
    {
        $worker = new Resque_Worker('*');
        $worker->registerWorker();

        // Make sure the worker is in the list
        $this->assertTrue((bool)$this->redis->sismember('workers', (string)$worker));
    }

    public function testGetAllWorkers()
    {
        $num = 3;
        // Register a few workers
        for($i = 0; $i < $num; ++$i) {
            $worker = new Resque_Worker('queue_' . $i);
            $worker->registerWorker();
        }

        // Now try to get them
        $this->assertEquals($num, count(Resque_Worker::all()));
    }

    public function testGetWorkerById()
    {
        $worker = new Resque_Worker('*');
        $worker->registerWorker();

        $newWorker = Resque_Worker::find((string)$worker);
        $this->assertEquals((string)$worker, (string)$newWorker);
    }

    public function testInvalidWorkerDoesNotExist()
    {
        $this->assertFalse(Resque_Worker::exists('blah'));
    }

    public function testWorkerCanUnregister()
    {
        $worker = new Resque_Worker('*');
        $worker->registerWorker();
        $worker->unregisterWorker();

        $this->assertFalse(Resque_Worker::exists((string)$worker));
        $this->assertEquals(array(), Resque_Worker::all());
        $this->assertEquals(array(), $this->redis->smembers('resque:workers'));
    }

    public function testPausedWorkerDoesNotPickUpJobs()
    {
        $worker = new Resque_Worker('*');
        $worker->pauseProcessing();
        Resque::enqueue('jobs', 'Test_Job');
        $worker->work(0);
        $worker->work(0);
        $this->assertEquals(0, Resque_Stat::get('processed'));
    }

    public function testResumedWorkerPicksUpJobs()
    {
        $worker = new Resque_Worker('*');
        $worker->pauseProcessing();
        Resque::enqueue('jobs', 'Test_Job');
        $worker->work(0);
        $this->assertEquals(0, Resque_Stat::get('processed'));
        $worker->unPauseProcessing();
        $worker->work(0);
        $this->assertEquals(1, Resque_Stat::get('processed'));
    }

    public function testWorkerCanWorkOverMultipleQueues()
    {
        $worker = new Resque_Worker(array(
            'queue1',
            'queue2'
        ));
        $worker->registerWorker();
        Resque::enqueue('queue1', 'Test_Job_1');
        Resque::enqueue('queue2', 'Test_Job_2');

        $job = $worker->reserve();
        $this->assertEquals('queue1', $job->queue);

        $job = $worker->reserve();
        $this->assertEquals('queue2', $job->queue);
    }

    public function testWorkerWorksQueuesInSpecifiedOrder()
    {
        $worker = new Resque_Worker(array(
            'high',
            'medium',
            'low'
        ));
        $worker->registerWorker();

        // Queue the jobs in a different order
        Resque::enqueue('low', 'Test_Job_1');
        Resque::enqueue('high', 'Test_Job_2');
        Resque::enqueue('medium', 'Test_Job_3');

        // Now check we get the jobs back in the right order
        $job = $worker->reserve();
        $this->assertEquals('high', $job->queue);

        $job = $worker->reserve();
        $this->assertEquals('medium', $job->queue);

        $job = $worker->reserve();
        $this->assertEquals('low', $job->queue);
    }

    public function testWildcardQueueWorkerWorksAllQueues()
    {
        $worker = new Resque_Worker('*');
        $worker->registerWorker();

        Resque::enqueue('queue1', 'Test_Job_1');
        Resque::enqueue('queue2', 'Test_Job_2');

        $job = $worker->reserve();
        $this->assertEquals('queue1', $job->queue);

        $job = $worker->reserve();
        $this->assertEquals('queue2', $job->queue);
    }

    public function testWorkerRetrievedJobBasedOnQos() {
        $worker = new Resque_Worker('*');
        $worker->registerWorker();
        $worker->logLevel = Resque_Worker::LOG_NONE;
        $worker->logOutput = fopen('php://memory', 'r+');

        Resque::enqueue('queue1', 'Test_Job_1_1');
        Resque::enqueue('queue1', 'Test_Job_1_2');

        Resque::enqueue('queue2', 'Test_Job_2_1');
        Resque::enqueue('queue2', 'Test_Job_2_2');

        $expectedOrder = ['queue1', 'queue2', 'queue1', 'queue2'];
        $i = 1;
        foreach ($expectedOrder as $expectedQueue) {
            sleep(1);// QoS is in a per-second resolution
            $job = $worker->reserve();
            $this->assertEquals($expectedQueue, $job->queue, "Queue $i should have been $expectedQueue");

            $i++;
        }
    }

    public function testWorkerRetrievedJobBasedOnQosWithOffset() {
        $worker = new Resque_Worker('*');
        $worker->registerWorker();
        $worker->logLevel = Resque_Worker::LOG_NONE;
        $worker->logOutput = fopen('php://memory', 'r+');

        Resque::enqueue('queue1', 'Test_Job_1_1');
        Resque::enqueue('queue1', 'Test_Job_1_2');
        Resque::enqueue('queue2', 'Test_Job_2_1');
        Resque::enqueue('queue2', 'Test_Job_2_2');
        Resque::enqueue('queue2', 'Test_Job_2_3');
        Resque::enqueue('queue3', 'Test_Job_3_1');
        Resque::enqueue('queue3', 'Test_Job_3_2');

        Resque::setOffset('queue1', 3);
        $expectedOrder = ['queue1', 'queue2', 'queue3', 'queue2', 'queue3', 'queue1', 'queue2'];
        $i = 1;
        foreach ($expectedOrder as $expectedQueue) {
            sleep(1);// QoS is in a per-second resolution
            $job = $worker->reserve();
            $this->assertEquals($expectedQueue, $job->queue, "Queue $i should have been $expectedQueue");
            $i++;
        }
    }

    public function testWorkerDoesNotWorkOnUnknownQueues()
    {
        $worker = new Resque_Worker('queue1');
        $worker->registerWorker();
        Resque::enqueue('queue2', 'Test_Job');

        $this->assertFalse($worker->reserve());
    }

    public function testWorkerClearsItsStatusWhenNotWorking()
    {
        Resque::enqueue('jobs', 'Test_Job');
        $worker = new Resque_Worker('jobs');
        $job = $worker->reserve();
        $worker->workingOn($job);
        $worker->doneWorking();
        $this->assertEquals(array(), $worker->job());
    }

    public function testWorkerRecordsWhatItIsWorkingOn()
    {
        $worker = new Resque_Worker('jobs');
        $worker->registerWorker();

        $payload = array(
            'class' => 'Test_Job'
        );
        $job = new Resque_Job('jobs', $payload);
        $worker->workingOn($job);

        $job = $worker->job();
        $this->assertEquals('jobs', $job['queue']);
        if(!isset($job['run_at'])) {
            $this->fail('Job does not have run_at time');
        }
        $this->assertEquals($payload, $job['payload']);
    }

    public function testWorkerErasesItsStatsWhenShutdown()
    {
        Resque::enqueue('jobs', 'Test_Job');
        Resque::enqueue('jobs', 'Invalid_Job');

        $worker = new Resque_Worker('jobs');
        $worker->work(0);
        $worker->work(0);

        $this->assertEquals(0, $worker->getStat('processed'));
        $this->assertEquals(0, $worker->getStat('failed'));
    }

    public function testWorkerCleansUpDeadWorkersOnStartup()
    {
        // Register a good worker
        $goodWorker = new Resque_Worker('jobs');
        $goodWorker->registerWorker();
        $workerId = explode(':', $goodWorker);

        // Register some bad workers
        $worker = new Resque_Worker('jobs');
        $worker->setId($workerId[0].':1:jobs');
        $worker->registerWorker();

        $worker = new Resque_Worker(array('high', 'low'));
        $worker->setId($workerId[0].':2:high,low');
        $worker->registerWorker();

        $this->assertEquals(3, count(Resque_Worker::all()));

        $goodWorker->pruneDeadWorkers();

        // There should only be $goodWorker left now
        $this->assertEquals(1, count(Resque_Worker::all()));
    }

    public function testDeadWorkerCleanUpDoesNotCleanUnknownWorkers()
    {
        // Register a bad worker on this machine
        $worker = new Resque_Worker('jobs');
        $workerId = explode(':', $worker);
        $worker->setId($workerId[0].':1:jobs');
        $worker->registerWorker();

        // Register some other false workers
        $worker = new Resque_Worker('jobs');
        $worker->setId('my.other.host:1:jobs');
        $worker->registerWorker();

        $this->assertEquals(2, count(Resque_Worker::all()));

        $worker->pruneDeadWorkers();

        // my.other.host should be left
        $workers = Resque_Worker::all();
        $this->assertEquals(1, count($workers));
        $this->assertEquals((string)$worker, (string)$workers[0]);
    }

    public function testWorkerFailsUncompletedJobsOnExit()
    {
        $worker = new Resque_Worker('jobs');
        $worker->registerWorker();

        $payload = array(
            'class' => 'Test_Job',
            'id' => 'randomId'
        );
        $job = new Resque_Job('jobs', $payload);

        $worker->workingOn($job);
        $worker->unregisterWorker();

        $this->assertEquals(1, Resque_Stat::get('failed'));
    }

    public function testWorkerLogAllMessageOnVerbose()
    {
        $worker = new Resque_Worker('jobs');
        $worker->logLevel = Resque_Worker::LOG_VERBOSE;
        $worker->logOutput = fopen('php://memory', 'r+');

        $message = array('message' => 'x', 'data' => '');

        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_DEBUG));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_INFO));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_WARNING));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_CRITICAL));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_ERROR));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_ALERT));

        rewind($worker->logOutput);
        $output = stream_get_contents($worker->logOutput);

        $lines = explode("\n", $output);
        $this->assertEquals(6, count($lines) -1);
    }

    public function testWorkerLogOnlyInfoMessageOnNonVerbose()
    {
        $worker = new Resque_Worker('jobs');
        $worker->logLevel = Resque_Worker::LOG_NORMAL;
        $worker->logOutput = fopen('php://memory', 'r+');

        $message = array('message' => 'x', 'data' => '');

        $this->assertEquals(false, $worker->log($message, Resque_Worker::LOG_TYPE_DEBUG));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_INFO));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_WARNING));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_CRITICAL));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_ERROR));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_ALERT));

        rewind($worker->logOutput);
        $output = stream_get_contents($worker->logOutput);

        $lines = explode("\n", $output);
        $this->assertEquals(5, count($lines) -1);
    }

    public function testWorkerLogNothingWhenLogNone()
    {
        $worker = new Resque_Worker('jobs');
        $worker->logLevel = Resque_Worker::LOG_NONE;
        $worker->logOutput = fopen('php://memory', 'r+');

        $message = array('message' => 'x', 'data' => '');

        $this->assertEquals(false, $worker->log($message, Resque_Worker::LOG_TYPE_DEBUG));
        $this->assertEquals(false, $worker->log($message, Resque_Worker::LOG_TYPE_INFO));
        $this->assertEquals(false, $worker->log($message, Resque_Worker::LOG_TYPE_WARNING));
        $this->assertEquals(false, $worker->log($message, Resque_Worker::LOG_TYPE_CRITICAL));
        $this->assertEquals(false, $worker->log($message, Resque_Worker::LOG_TYPE_ERROR));
        $this->assertEquals(false, $worker->log($message, Resque_Worker::LOG_TYPE_ALERT));

        rewind($worker->logOutput);
        $output = stream_get_contents($worker->logOutput);

        $lines = explode("\n", $output);
        $this->assertEquals(0, count($lines) -1);
    }

    public function testWorkerLogWithISOTime()
    {
        $worker = new Resque_Worker('jobs');
        $worker->logLevel = Resque_Worker::LOG_NORMAL;
        $worker->logOutput = fopen('php://memory', 'r+');

        $message = array('message' => 'x', 'data' => '');

        $now = date('c');
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_INFO));

        rewind($worker->logOutput);
        $output = stream_get_contents($worker->logOutput);

        $lines = explode("\n", $output);
        $this->assertEquals(1, count($lines) -1);
        $this->assertEquals('[' . $now . '] x', $lines[0]);
    }

    public function testTimeoutRetry(){
        $worker = new Resque_Worker('jobs');
        $worker->registerWorker();
        $worker->setMaxRetries(2);
        //no timeout without fork
        $worker->setFork(false);
        $args = ['sleep' => 2];
        Resque::enqueue('jobs', 'Test_Job_Timeout', $args);

        $worker->work(0);
        $this->assertEquals(1, Resque_Stat::get('success'));

        //timeout with fork
        $worker->setFork(true);
        Resque::enqueue('jobs', 'Test_Job_Timeout', $args);
        $worker->work(0);
        $this->assertEquals(2, Resque_Stat::get('failed'));
        $this->assertEquals(1, Resque::getFailedCount());

        $failedItems = Resque::getFailed();
        $this->assertCount(1, $failedItems);

        //timeout without sleep
        $args = ['sleep' => 0];
        Resque::enqueue('jobs', 'Test_Job_Timeout', $args);
        $worker->work(0);
        $this->assertEquals(2, Resque_Stat::get('success'));

    }

}
