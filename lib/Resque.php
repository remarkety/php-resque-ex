<?php
require_once dirname(__FILE__) . '/Resque/Event.php';
require_once dirname(__FILE__) . '/Resque/Exception.php';

/**
 * Base Resque class.
 *
 * @package		Resque
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class Resque
{
	const VERSION = '1.2.5';

	/**
	 * @var Resque_Redis Instance of Resque_Redis that talks to redis.
	 */
	public static $redis = null;

	/**
	 * @var mixed Host/port conbination separated by a colon, or a nested
	 * array of server swith host/port pairs
	 */
	protected static $redisServer = null;

	/**
	 * @var int ID of Redis database to select.
	 */
	protected static $redisDatabase = 0;

	/**
	 * @var string namespace of the redis keys
	 */
	public static $namespace = '';

	/**
	 * @var string password for the redis server
	 */
	protected static $password = null;

	/**
	 * @var int PID of current process. Used to detect changes when forking
	 *  and implement "thread" safety to avoid race conditions.
	 */
	 protected static $pid = null;

	/**
	 * @var bool Added by Guy - Support QOS when getting the next queue to process
	 */
	public static $useQos = true;

	/**
	 * Given a host/port combination separated by a colon, set it as
	 * the redis server that Resque will talk to.
	 *
	 * @param mixed $server Host/port combination separated by a colon, or
	 *                      a nested array of servers with host/port pairs.
	 * @param int $database
	 */
	public static function setBackend($server, $database = 0, $namespace = 'resque', $password = null)
	{
		self::$redisServer   = $server;
		self::$redisDatabase = $database;
		self::$redis         = null;
		self::$namespace 	 = $namespace;
		self::$password 	 = $password;
	}

	/**
	 * Return an instance of the Resque_Redis class instantiated for Resque.
	 *
	 * @return Resque_Redis Instance of Resque_Redis.
	 */
	public static function redis()
	{
		// Detect when the PID of the current process has changed (from a fork, etc)
		// and force a reconnect to redis.
		$pid = getmypid();
		if (self::$pid !== $pid) {
			self::$redis = null;
			self::$pid   = $pid;
		}

		if(!is_null(self::$redis)) {
			return self::$redis;
		}

		$server = self::$redisServer;
		if (empty($server)) {
			$server = 'localhost:6379';
		}

		if(is_array($server)) {
			require_once dirname(__FILE__) . '/Resque/RedisCluster.php';
			self::$redis = new Resque_RedisCluster($server);
		}
		else {
			if (strpos($server, 'unix:') === false) {
				list($host, $port) = explode(':', $server);
			}
			else {
				$host = $server;
				$port = null;
			}
			require_once dirname(__FILE__) . '/Resque/Redis.php';
			$redisInstance = new Resque_Redis($host, $port, self::$password);
			$redisInstance->prefix(self::$namespace);
			self::$redis = $redisInstance;
		}

		if(self::$redisDatabase !== 0) {
			self::$redis->select(self::$redisDatabase);
		}

		return self::$redis;
	}

	/**
	 * Push a job to the end of a specific queue. If the queue does not
	 * exist, then create it as well.
	 *
	 * @param string $queue The name of the queue to add the job to.
	 * @param array $item Job description as an array to be JSON encoded.
	 */
	public static function push($queue, $item)
	{
		if (self::$useQos) {
			$score = time();
			self::redis()->zadd('queues', $score, $queue);
		} else {
			self::redis()->sadd('queues', $queue);
		}
		self::redis()->rpush('queue:' . $queue, json_encode($item));

	}

	/**
	 * Pop an item off the end of the specified queue, decode it and
	 * return it.
	 *
	 * @param string $queue The name of the queue to fetch an item from.
	 * @return array Decoded item from the queue.
	 */
	public static function pop($queue)
	{
		$item = self::redis()->lpop('queue:' . $queue);
		if(!$item) {
			return;
		}

		return json_decode($item, true);
	}

	/**
	 * Return the size (number of pending jobs) of the specified queue.
	 *
	 * @param $queue name of the queue to be checked for pending jobs
	 *
	 * @return int The size of the queue.
	 */
	public static function size($queue)
	{
		return self::redis()->llen('queue:' . $queue);
	}

	/**
	 * Create a new job and save it to the specified queue.
	 *
	 * @param string $queue The name of the queue to place the job in.
	 * @param string $class The name of the class that contains the code to execute the job.
	 * @param array $args Any optional arguments that should be passed when the job is executed.
	 * @param boolean $trackStatus Set to true to be able to monitor the status of a job.
	 *
	 * @return string
	 */
	public static function enqueue($queue, $class, $args = null, $trackStatus = false)
	{
		require_once dirname(__FILE__) . '/Resque/Job.php';
		$jobId = Resque_Job::create($queue, $class, $args, $trackStatus);
		if ($jobId) {
			Resque_Event::trigger('afterEnqueue', array(
				'class' => $class,
				'args'  => $args,
				'queue' => $queue,
			));
		}
		\Resque_Stat::incr('pending');

		return $jobId;
	}

	public static function setOffset($queue, $offset = 0) {
        self::redis()->set('scoreOffset:'.$queue, (int)$offset);
    }

    public static function getOffset($queue) {
        $offset = self::redis()->get('scoreOffset:'.$queue);
        return empty($offset) ? 0 : (int)$offset;
    }

	public static function isJobPresent($queue) {
        return Resque::size($queue) > 0;
    }
	/**
	 * Reserve and return the next available job in the specified queue.
	 *
	 * @param string $queue Queue to fetch next available job from.
	 * @return Resque_Job Instance of Resque_Job to be processed, false if none or error.
	 */
	public static function reserve($queue, $scoreOffset = 0)
	{
		require_once dirname(__FILE__) . '/Resque/Job.php';
		$job = Resque_Job::reserve($queue, $scoreOffset);
		return $job;
	}

	/**
	 * Get an array of all known queues.
	 *
	 * @return array Array of queues.
	 */
	public static function queues($substr = null)
	{
		if (self::$useQos) {
			$queues = self::redis()->zrange('queues', 0, -1);
		} else {
			$queues = self::redis()->smembers('queues');
		}
		if(!is_array($queues)) {
			$queues = array();
		}
		if ($substr) {
			$queues = array_filter($queues, function($item) use ($substr) {
			    return strstr($item,$substr) !== FALSE;
			});
		}
		return $queues;
	}

	public static function failed($originalQueue, $class, $args){

        if(isset($args['id'])) {
            $id = $args['id'];
            unset($args['id']);
        } else {
            $id = md5(uniqid('', true));
        }
        $item = array(
            'originalQueue' => $originalQueue,
            'class'	=> $class,
            'args'	=> array($args),
            'id'	=> $id,
        );

        self::redis()->rpush('queue:failed', json_encode($item));
    }

    public static function getFailedCount(){
        return self::redis()->llen('queue:failed');
    }

    public static function getFailed($start = 0, $limit = null){
        if(is_null($limit)){
            $limit = self::getFailedCount();
        }
        $items = [];
        for($i = $start; $i < $limit+$start; $i++){
            $item = self::redis()->lindex('queue:failed', $i);
            $items[] = json_decode($item);
        }
        return $items;
    }
}
