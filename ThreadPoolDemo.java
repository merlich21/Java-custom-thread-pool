/**
 * Демонстрационный класс для иллюстрации работы пользовательского пула потоков.
 * Показывает различные сценарии использования и нагрузки на пул.
 */
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ThreadPoolDemo {
    // Логгер для отслеживания выполнения демонстрации
    private static final Logger logger = Logger.getLogger(ThreadPoolDemo.class.getName());
    // Счетчики для статистики выполнения задач
    private static final AtomicInteger completedTasks = new AtomicInteger(0);  // Количество выполненных задач
    private static final AtomicInteger rejectedTasks = new AtomicInteger(0);   // Количество отклоненных задач

    public static void main(String[] args) {
        // Настройка уровня логирования для всех логгеров
        Logger.getLogger("").setLevel(Level.ALL);

        // Создание пула потоков с тестовыми параметрами
        CustomThreadPool pool = new CustomThreadPool(
            2,  // corePoolSize - базовое количество потоков
            4,  // maxPoolSize - максимальное количество потоков
            5,  // keepAliveTime - время простоя потока
            TimeUnit.SECONDS,  // единицы измерения времени
            5,  // queueSize - размер очереди задач
            1   // minSpareThreads - минимальное количество резервных потоков
        );

        logger.info("Запуск демонстрации работы пула потоков...");

        // Сценарий 1: Нормальная работа при умеренной нагрузке
        logger.info("\nСценарий 1: Нормальная работа при умеренной нагрузке");
        submitTasks(pool, 10, 1000);  // 10 задач по 1 секунде каждая
        waitForTasks(15);  // Ожидание 15 секунд для завершения

        // Сценарий 2: Высокая нагрузка с возможными отказами
        logger.info("\nСценарий 2: Высокая нагрузка с возможными отказами");
        submitTasks(pool, 20, 500);   // 20 задач по 0.5 секунды каждая
        waitForTasks(15);

        // Сценарий 3: Резкий всплеск задач
        logger.info("\nСценарий 3: Резкий всплеск задач");
        submitTasks(pool, 30, 200);   // 30 задач по 0.2 секунды каждая
        waitForTasks(15);

        // Сценарий 4: Длительные задачи
        logger.info("\nСценарий 4: Длительные задачи");
        submitTasks(pool, 5, 5000);   // 5 задач по 5 секунд каждая
        waitForTasks(10);

        // Завершение работы пула
        logger.info("\nИнициирование завершения работы пула...");
        pool.shutdown();

        // Вывод итоговой статистики
        logger.info("\nИтоговая статистика:");
        logger.info("Всего выполнено задач: " + completedTasks.get());
        logger.info("Всего отклонено задач: " + rejectedTasks.get());
    }

    /**
     * Отправка набора задач в пул потоков
     * @param pool пул потоков для выполнения задач
     * @param count количество задач для отправки
     * @param sleepTime время выполнения каждой задачи в миллисекундах
     */
    private static void submitTasks(CustomThreadPool pool, int count, int sleepTime) {
        for (int i = 0; i < count; i++) {
            final int taskId = i;
            try {
                pool.execute(() -> {
                    logger.info("Задача " + taskId + " начата");
                    try {
                        Thread.sleep(sleepTime);  // Имитация работы задачи
                        completedTasks.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    logger.info("Задача " + taskId + " завершена");
                });
            } catch (Exception e) {
                rejectedTasks.incrementAndGet();
                logger.warning("Задача " + taskId + " отклонена");
            }
        }
    }

    /**
     * Ожидание заданного количества секунд
     * @param seconds количество секунд для ожидания
     */
    private static void waitForTasks(int seconds) {
        try {
            logger.info("Ожидание " + seconds + " секунд...");
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
} 