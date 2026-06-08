/**
 * Реализация пользовательского пула потоков с расширенными возможностями управления
 * и балансировки нагрузки. Данная реализация предоставляет гибкую настройку параметров,
 * подробное логирование и механизмы обработки отказов.
 */
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class CustomThreadPool implements Executor {
    // Логгер для отслеживания всех событий пула потоков
    private static final Logger logger = Logger.getLogger(CustomThreadPool.class.getName());
    
    // Основные параметры конфигурации пула
    private final int corePoolSize;        // Минимальное количество потоков
    private final int maxPoolSize;         // Максимальное количество потоков
    private final long keepAliveTime;      // Время простоя потока до завершения
    private final TimeUnit timeUnit;       // Единицы измерения времени
    private final int queueSize;           // Размер очереди задач
    private final int minSpareThreads;     // Минимальное количество резервных потоков
    
    // Внутренние компоненты пула
    private final List<Worker> workers;    // Список рабочих потоков
    private final List<BlockingQueue<Runnable>> queues;  // Очереди задач для каждого потока
    private final CustomThreadFactory threadFactory;     // Фабрика для создания потоков
    private final RejectedExecutionHandler rejectionHandler;  // Обработчик отказов
    
    // Состояние пула
    private volatile boolean isShutdown = false;  // Флаг завершения работы пула
    private final ReentrantLock mainLock = new ReentrantLock();  // Основная блокировка
    private final AtomicInteger currentPoolSize = new AtomicInteger(0);  // Текущий размер пула
    private final AtomicInteger activeThreads = new AtomicInteger(0);    // Количество активных потоков
    private final AtomicInteger nextQueueIndex = new AtomicInteger(0);   // Индекс для round-robin распределения

    /**
     * Конструктор пула потоков
     * @param corePoolSize минимальное количество потоков
     * @param maxPoolSize максимальное количество потоков
     * @param keepAliveTime время простоя потока
     * @param timeUnit единицы измерения времени
     * @param queueSize размер очереди задач
     * @param minSpareThreads минимальное количество резервных потоков
     * @throws IllegalArgumentException при некорректных параметрах
     */
    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime, 
                          TimeUnit timeUnit, int queueSize, int minSpareThreads) {
        // Проверка корректности входных параметров
        if (corePoolSize < 0 || maxPoolSize <= 0 || maxPoolSize < corePoolSize || 
            keepAliveTime < 0 || queueSize <= 0 || minSpareThreads < 0) {
            throw new IllegalArgumentException("Некорректные параметры пула потоков");
        }
        
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        
        // Инициализация компонентов пула
        this.workers = new ArrayList<>(maxPoolSize);
        this.queues = new ArrayList<>(maxPoolSize);
        this.threadFactory = new CustomThreadFactory();
        this.rejectionHandler = new CustomRejectionHandler();
        
        initializePool();
    }
    
    /**
     * Инициализация пула потоков - создание базовых потоков
     */
    private void initializePool() {
        for (int i = 0; i < corePoolSize; i++) {
            createWorker();
        }
        logger.info("[Pool] Пул потоков инициализирован с " + corePoolSize + " базовыми потоками");
    }
    
    /**
     * Создание нового рабочего потока и его очереди
     */
    private void createWorker() {
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);
        queues.add(queue);
        Worker worker = new Worker(queue);
        workers.add(worker);
        Thread thread = threadFactory.newThread(worker);
        thread.start();
        currentPoolSize.incrementAndGet();
        logger.info("[ThreadFactory] Создан новый поток: " + thread.getName());
    }
    
    /**
     * Выполнение задачи в пуле потоков
     * @param task задача для выполнения
     * @throws NullPointerException если задача равна null
     */
    @Override
    public void execute(Runnable task) {
        if (task == null) {
            throw new NullPointerException("Задача не может быть null");
        }
        
        if (isShutdown) {
            rejectionHandler.rejectedExecution(task, null);
            return;
        }
        
        mainLock.lock();
        try {
            // Проверка необходимости создания новых потоков
            int activeCount = activeThreads.get();
            int currentSize = currentPoolSize.get();
            
            // Создаем новый поток, если все текущие потоки заняты и не достигнут максимум
            if (activeCount >= currentSize && currentSize < maxPoolSize) {
                createWorker();
                logger.info("[Pool] Создан новый рабочий поток. Текущий размер пула: " + currentSize);
            }
            
            // Выбор очереди для задачи
            BlockingQueue<Runnable> targetQueue = getTargetQueue();
            
            if (!targetQueue.offer(task)) {
                rejectionHandler.rejectedExecution(task, null);
            } else {
                logger.info("[Pool] Задача принята в очередь #" + queues.indexOf(targetQueue) + ": " + task);
            }
        } finally {
            mainLock.unlock();
        }
    }
    
    /**
     * Получение очереди для новой задачи.
     * Реализует алгоритм Round Robin для распределения задач
     * @return очередь для размещения задачи
     */
    private BlockingQueue<Runnable> getTargetQueue() {
        // Реализация Round Robin
        int index = nextQueueIndex.getAndIncrement() % queues.size();
        return queues.get(index);
    }
    
    /**
     * Плавное завершение работы пула потоков
     * Существующие задачи будут выполнены, новые задачи отклоняются
     */
    public void shutdown() {
        mainLock.lock();
        try {
            isShutdown = true;
            for (Worker worker : workers) {
                worker.interrupt();
            }
            logger.info("[Pool] Инициировано завершение работы пула потоков");
        } finally {
            mainLock.unlock();
        }
    }
    
    /**
     * Немедленное завершение работы пула потоков
     * Все задачи отменяются, потоки прерываются
     */
    public void shutdownNow() {
        mainLock.lock();
        try {
            isShutdown = true;
            for (Worker worker : workers) {
                worker.interrupt();
            }
            for (BlockingQueue<Runnable> queue : queues) {
                queue.clear();
            }
            logger.info("[Pool] Немедленное завершение работы пула потоков");
        } finally {
            mainLock.unlock();
        }
    }
    
    /**
     * Внутренний класс, представляющий рабочий поток пула
     */
    private class Worker implements Runnable {
        private final BlockingQueue<Runnable> queue;  // Очередь задач для этого потока
        private volatile boolean running = true;      // Флаг активности потока
        
        public Worker(BlockingQueue<Runnable> queue) {
            this.queue = queue;
        }
        
        /**
         * Прерывание работы потока
         */
        public void interrupt() {
            running = false;
            Thread.currentThread().interrupt();
        }
        
        @Override
        public void run() {
            while (running) {
                try {
                    // Ожидание задачи с таймаутом
                    Runnable task = queue.poll(keepAliveTime, timeUnit);
                    if (task != null) {
                        activeThreads.incrementAndGet();
                        try {
                            logger.info("[Worker] " + Thread.currentThread().getName() + " выполняет задачу: " + task);
                            task.run();
                        } finally {
                            activeThreads.decrementAndGet();
                        }
                    } else if (currentPoolSize.get() > corePoolSize) {
                        // Если поток простаивает и превышено базовое количество потоков,
                        // завершаем его работу
                        logger.info("[Worker] " + Thread.currentThread().getName() + " простаивает, завершение работы");
                        break;
                    }
                } catch (InterruptedException e) {
                    if (!running) {
                        break;
                    }
                }
            }
            
            // Очистка ресурсов при завершении потока
            currentPoolSize.decrementAndGet();
            workers.remove(this);
            logger.info("[Worker] " + Thread.currentThread().getName() + " завершил работу. Текущий размер пула: " + currentPoolSize.get());
        }
    }
    
    /**
     * Фабрика для создания потоков с уникальными именами
     */
    private static class CustomThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "CustomThreadPool-Worker-" + threadNumber.getAndIncrement());
            t.setDaemon(false);
            logger.info("[ThreadFactory] Создан новый поток: " + t.getName());
            return t;
        }
    }
    
    /**
     * Обработчик отказов при переполнении очереди
     */
    private static class CustomRejectionHandler implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            logger.warning("[Rejected] Задача отклонена из-за перегрузки: " + r.toString());
            // В данной реализации просто логируем отказ
            // Можно реализовать различные стратегии обработки отказов:
            // 1. Выполнение в текущем потоке
            // 2. Отбрасывание задачи
            // 3. Повторная попытка через некоторое время
        }
    }
}
