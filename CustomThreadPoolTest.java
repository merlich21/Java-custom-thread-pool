/**
 * Демонстрационный класс для тестирования пользовательского пула потоков.
 * Включает различные сценарии использования и проверки работоспособности.
 */
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CustomThreadPoolTest {
    private static final Logger logger = Logger.getLogger(CustomThreadPoolTest.class.getName());
    private static final AtomicInteger completedTasks = new AtomicInteger(0);
    private static final AtomicInteger rejectedTasks = new AtomicInteger(0);

    public static void main(String[] args) {
        // Настройка логирования
        Logger.getLogger("").setLevel(Level.ALL);

        // Создание пула потоков с тестовыми параметрами
        CustomThreadPool pool = new CustomThreadPool(
            2,  // corePoolSize - базовое количество потоков
            4,  // maxPoolSize - максимальное количество потоков
            5,  // keepAliveTime - время простоя потока
            TimeUnit.SECONDS,  // timeUnit - единицы измерения времени
            10, // queueSize - размер очереди задач
            1   // minSpareThreads - минимальное количество резервных потоков
        );

        logger.info("=== Начало тестирования пула потоков ===");
        
        // Тест 1: Базовая нагрузка
        logger.info("\n=== Тест 1: Базовая нагрузка ===");
        submitTasks(pool, 10, 1000, "Базовая задача");
        
        // Тест 2: Перегрузка пула
        logger.info("\n=== Тест 2: Перегрузка пула ===");
        submitTasks(pool, 30, 2000, "Задача под нагрузкой");
        
        // Тест 3: Проверка работы с разными типами задач
        logger.info("\n=== Тест 3: Разные типы задач ===");
        submitDifferentTasks(pool);
        
        // Ожидание завершения всех задач
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Вывод статистики
        logger.info("\n=== Статистика выполнения ===");
        logger.info("Выполнено задач: " + completedTasks.get());
        logger.info("Отклонено задач: " + rejectedTasks.get());
        
        // Плавное завершение работы пула
        logger.info("\n=== Завершение работы пула ===");
        pool.shutdown();
        
        // Проверка немедленного завершения
        logger.info("\n=== Тест немедленного завершения ===");
        CustomThreadPool immediatePool = new CustomThreadPool(2, 4, 5, TimeUnit.SECONDS, 10, 1);
        submitTasks(immediatePool, 5, 1000, "Задача для немедленного завершения");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        immediatePool.shutdownNow();
    }

    /**
     * Отправка набора задач в пул потоков
     * @param pool пул потоков
     * @param taskCount количество задач
     * @param sleepTime время выполнения задачи
     * @param taskName имя задачи
     */
    private static void submitTasks(CustomThreadPool pool, int taskCount, long sleepTime, String taskName) {
        for (int i = 0; i < taskCount; i++) {
            final int taskId = i;
            try {
                pool.execute(() -> {
                    try {
                        logger.info("[Task] " + taskName + " #" + taskId + " начата");
                        Thread.sleep(sleepTime);
                        completedTasks.incrementAndGet();
                        logger.info("[Task] " + taskName + " #" + taskId + " завершена");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.warning("[Task] " + taskName + " #" + taskId + " прервана");
                    }
                });
            } catch (Exception e) {
                rejectedTasks.incrementAndGet();
                logger.warning("[Task] " + taskName + " #" + taskId + " отклонена: " + e.getMessage());
            }
        }
    }

    /**
     * Отправка различных типов задач для тестирования
     * @param pool пул потоков
     */
    private static void submitDifferentTasks(CustomThreadPool pool) {
        // Задача с исключением
        pool.execute(() -> {
            logger.info("[Task] Задача с исключением начата");
            throw new RuntimeException("Тестовое исключение");
        });

        // Длительная задача
        pool.execute(() -> {
            logger.info("[Task] Длительная задача начата");
            try {
                Thread.sleep(5000);
                logger.info("[Task] Длительная задача завершена");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Быстрая задача
        pool.execute(() -> {
            logger.info("[Task] Быстрая задача выполнена");
        });

        // Задача с вложенными задачами
        pool.execute(() -> {
            logger.info("[Task] Задача с вложенными задачами начата");
            for (int i = 0; i < 3; i++) {
                final int nestedId = i;
                pool.execute(() -> {
                    logger.info("[Task] Вложенная задача #" + nestedId + " выполнена");
                });
            }
            logger.info("[Task] Задача с вложенными задачами завершена");
        });
    }
} 