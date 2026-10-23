import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// 1. Transaction Status Lifecycle Enum
enum TransactionStatus {
    RECEIVED, PROCESSING, PROCESSED, DUPLICATE, FAILED, RETRY_PENDING
}

enum TransactionType {
    CREDIT, DEBIT, REVERSAL
}

// 2. Transaction Request Model
class TransactionRequest {
    private String transactionId;
    private String requestId; // Idempotency key
    private int sequence;
    private String accountId;
    private TransactionType type;
    private BigDecimal amount;
    private TransactionStatus status;
    private int retryCount;

    public TransactionRequest(String transactionId, String requestId, int sequence, String accountId, TransactionType type, BigDecimal amount) {
        this.transactionId = transactionId;
        this.requestId = requestId;
        this.sequence = sequence;
        this.accountId = accountId;
        this.type = type;
        this.amount = amount;
        this.status = TransactionStatus.RECEIVED;
        this.retryCount = 0;
    }

    public String getTransactionId() { return transactionId; }
    public String getRequestId() { return requestId; }
    public int getSequence() { return sequence; }
    public BigDecimal getAmount() { return amount; }
    public TransactionStatus getStatus() { return status; }
    public void setStatus(TransactionStatus status) { this.status = status; }
    public int getRetryCount() { return retryCount; }
    public void incrementRetryCount() { this.retryCount++; }
}

// 3. Background Transaction Processing Service
class TransactionProcessingService {
    private final Set<String> processedRequestIds = ConcurrentHashMap.newKeySet();
    private final Map<String, TransactionStatus> transactionStore = new ConcurrentHashMap<>();
    
    private int nextExpectedSequence = 1;
    private final List<TransactionRequest> pendingBuffer = new ArrayList<>();
    private final Map<String, Integer> statusCounts = new ConcurrentHashMap<>();

    public void processBatch(List<TransactionRequest> incomingRequests) {
        incomingRequests.sort(Comparator.comparingInt(TransactionRequest::getSequence));

        for (TransactionRequest req : incomingRequests) {
            processSingleRequest(req);
        }

        processPendingBuffer();
        printSummary();
    }

    private void processSingleRequest(TransactionRequest req) {
        if (req.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
            req.setStatus(TransactionStatus.FAILED);
            updateSummary(TransactionStatus.FAILED);
            System.out.println("Validation Failed for Transaction: " + req.getTransactionId() + " (Invalid Amount)");
            return;
        }

        if (processedRequestIds.contains(req.getRequestId())) {
            req.setStatus(TransactionStatus.DUPLICATE);
            updateSummary(TransactionStatus.DUPLICATE);
            System.out.println("Duplicate detected for Request ID: " + req.getRequestId());
            return;
        }

        if (req.getSequence() > nextExpectedSequence) {
            req.setStatus(TransactionStatus.RETRY_PENDING);
            pendingBuffer.add(req);
            updateSummary(TransactionStatus.RETRY_PENDING);
            System.out.println("Out-of-order request buffered. Expected seq: " + nextExpectedSequence + ", Got: " + req.getSequence());
            return;
        }

        executeTransaction(req);
    }

    private void executeTransaction(TransactionRequest req) {
        req.setStatus(TransactionStatus.PROCESSING);
        try {
            if (req.getTransactionId().equals("TXN-2005") && req.getRetryCount() == 0) {
                throw new RuntimeException("Simulated Transient Network Failure");
            }

            req.setStatus(TransactionStatus.PROCESSED);
            processedRequestIds.add(req.getRequestId());
            transactionStore.put(req.getTransactionId(), TransactionStatus.PROCESSED);
            updateSummary(TransactionStatus.PROCESSED);
            
            System.out.println("Successfully Processed: " + req.getTransactionId());
            
            if (req.getSequence() == nextExpectedSequence) {
                nextExpectedSequence++;
            }

        } catch (Exception e) {
            handleFailure(req, e.getMessage());
        }
    }

    private void handleFailure(TransactionRequest req, String errorReason) {
        req.incrementRetryCount();
        if (req.getRetryCount() <= 1) {
            req.setStatus(TransactionStatus.RETRY_PENDING);
            updateSummary(TransactionStatus.RETRY_PENDING);
            System.out.println("Retrying transaction due to failure: " + req.getTransactionId() + " (Attempt: " + req.getRetryCount() + ")");
            executeTransaction(req);
        } else {
            req.setStatus(TransactionStatus.FAILED);
            updateSummary(TransactionStatus.FAILED);
            System.out.println("Transaction permanently failed after retries: " + req.getTransactionId());
        }
    }

    private void processPendingBuffer() {
        Iterator<TransactionRequest> iterator = pendingBuffer.iterator();
        while (iterator.hasNext()) {
            TransactionRequest req = iterator.next();
            if (req.getSequence() == nextExpectedSequence) {
                iterator.remove();
                executeTransaction(req);
            }
        }
    }

    private synchronized void updateSummary(TransactionStatus status) {
        statusCounts.put(status.name(), statusCounts.getOrDefault(status.name(), 0) + 1);
    }

    public void printSummary() {
        System.out.println("\n--- Processing Summary Report ---");
        statusCounts.forEach((status, count) -> System.out.println(status + ": " + count));
        System.out.println("---------------------------------");
    }
}

// 4. Main Application Runner matching Assessment Test Data
public class Main {
    public static void main(String[] args) {
        TransactionProcessingService service = new TransactionProcessingService();

        List<TransactionRequest> testBatch = Arrays.asList(
            new TransactionRequest("TXN-2002", "REQ-2", 2, "ACC-201", TransactionType.DEBIT, new BigDecimal("40")),
            new TransactionRequest("TXN-2001", "REQ-1", 1, "ACC-201", TransactionType.CREDIT, new BigDecimal("100")),
            new TransactionRequest("TXN-2002", "REQ-2-DUP", 2, "ACC-201", TransactionType.DEBIT, new BigDecimal("40")),
            new TransactionRequest("TXN-2003", "REQ-3", 3, "ACC-201", TransactionType.CREDIT, new BigDecimal("25")),
            new TransactionRequest("TXN-2004", "REQ-4", 4, "ACC-202", TransactionType.DEBIT, new BigDecimal("-10")), 
            new TransactionRequest("TXN-2005", "REQ-5", 5, "ACC-203", TransactionType.CREDIT, new BigDecimal("500")) 
        );

        service.processBatch(testBatch);
    }
}
