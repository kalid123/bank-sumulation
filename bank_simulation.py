import threading
import random
import time

# Constants
NUM_TELLERS = 3
NUM_CUSTOMERS = 50

# Convert milliseconds to seconds
def ms_delay(low, high):
    return random.uniform(low/1000, high/1000)

# Semaphores for shared resources
door = threading.Semaphore(2)             # Only 2 customers can enter at a time.
safe = threading.Semaphore(2)             # Only 2 tellers can be in the safe concurrently.
manager = threading.Semaphore(1)          # Only 1 teller at a time with the manager.
teller_available = threading.Semaphore(NUM_TELLERS)  # Teller availability.

# Global variables
bank_open = threading.Event()             # Set when all tellers are ready.
customers_served = 0
customers_served_lock = threading.Lock()

# Teller assignment management
available_tellers = []
teller_lock = threading.Lock()

# Per-teller communication semaphores and shared data
customer_arrived = [threading.Semaphore(0) for _ in range(NUM_TELLERS)]
transaction_provided = [threading.Semaphore(0) for _ in range(NUM_TELLERS)]
transaction_done = [threading.Semaphore(0) for _ in range(NUM_TELLERS)]
customer_left = [threading.Semaphore(0) for _ in range(NUM_TELLERS)]
teller_transaction = [None for _ in range(NUM_TELLERS)]

# Logging function: prints messages in the desired format.
def log(thread_type, id, partner_type, partner_id, msg):
    print(f"{thread_type} {id} [{partner_type} {partner_id}]: {msg}")

# Teller thread function
def teller(teller_id):
    global customers_served
    # Teller announces readiness
    log("Teller", teller_id, "[]", "", "ready to serve")
    log("Teller", teller_id, "[]", "", "waiting for a customer")
    
    with teller_lock:
        available_tellers.append(teller_id)
        if len(available_tellers) == NUM_TELLERS:
            bank_open.set()  # All tellers ready, bank opens.
    
    while True:
        # Wait for a customer to be assigned
        customer_arrived[teller_id].acquire()
        
        with customers_served_lock:
            if customers_served >= NUM_CUSTOMERS:
                break  # No more customers.
        
        log("Teller", teller_id, "Customer", "-", "asks for transaction")
        transaction_provided[teller_id].acquire()  # Wait for customer to provide transaction
        transaction = teller_transaction[teller_id]
        
        # If withdrawal, get manager's permission
        if transaction == "Withdraw":
            log("Teller", teller_id, "Manager", "-", "requesting manager permission")
            manager.acquire()
            log("Teller", teller_id, "Manager", "-", "got manager's permission (interaction start)")
            time.sleep(ms_delay(5, 30))
            log("Teller", teller_id, "Manager", "-", "manager interaction complete")
            manager.release()
        
        # Go to the safe
        log("Teller", teller_id, "Safe", "-", "going to safe")
        safe.acquire()
        log("Teller", teller_id, "Safe", "-", "enter safe")
        time.sleep(ms_delay(10, 50))
        log("Teller", teller_id, "Safe", "-", f"finishes {transaction.lower()} transaction")
        safe.release()
        
        log("Teller", teller_id, "Customer", "-", "notifying transaction complete, waiting for customer to leave")
        transaction_done[teller_id].release()  # Notify customer
        
        # Wait for customer to leave
        customer_left[teller_id].acquire()
        
        with customers_served_lock:
            customers_served += 1
        
        # After serving, mark self available again
        with teller_lock:
            available_tellers.append(teller_id)
            teller_available.release()
    
    log("Teller", teller_id, "[]", "", "leaving for the day")

# Customer thread function
def customer(customer_id):
    transaction = random.choice(["Deposit", "Withdraw"])
    log("Customer", customer_id, "[]", "", f"wants to perform a {transaction.lower()} transaction")
    time.sleep(ms_delay(0, 100))  # Wait before arriving
    
    log("Customer", customer_id, "[]", "", "going to bank")
    door.acquire()
    log("Customer", customer_id, "[]", "", "entering bank")
    log("Customer", customer_id, "[]", "", "getting in line")
    
    # Wait until the bank is open
    bank_open.wait()
    
    # Wait for an available teller
    teller_available.acquire()
    with teller_lock:
        teller_id = available_tellers.pop(0)
    log("Customer", customer_id, "[]", "", "selecting a teller")
    log("Customer", customer_id, f"Teller", teller_id, "selects teller")
    log("Customer", customer_id, f"Teller", teller_id, "introduces itself")
    
    # Signal to the assigned teller that customer has arrived
    customer_arrived[teller_id].release()
    time.sleep(0.001)
    
    log("Customer", customer_id, f"Teller", teller_id, f"asks for {transaction.lower()} transaction")
    teller_transaction[teller_id] = transaction
    transaction_provided[teller_id].release()
    
    # Wait for teller to finish transaction
    transaction_done[teller_id].acquire()
    log("Customer", customer_id, f"Teller", teller_id, "leaves teller")
    log("Customer", customer_id, "[]", "", "goes to door")
    log("Customer", customer_id, "[]", "", "leaves the bank")
    customer_left[teller_id].release()
    door.release()

# --- Main simulation start ---

# Create and start teller threads.
teller_threads = []
for tid in range(NUM_TELLERS):
    t = threading.Thread(target=teller, args=(tid,))
    t.start()
    teller_threads.append(t)

# Wait for all tellers to be ready (bank opens when all are ready)
bank_open.wait()

# Create and start customer threads.
customer_threads = []
for cid in range(NUM_CUSTOMERS):
    c = threading.Thread(target=customer, args=(cid,))
    c.start()
    customer_threads.append(c)

# Wait for all customer threads to finish.
for c in customer_threads:
    c.join()

# After serving all customers, signal tellers to exit.
# Release each teller's waiting semaphore so they exit their loops.
for _ in range(NUM_TELLERS):
    for tid in range(NUM_TELLERS):
        customer_arrived[tid].release()

# Wait for all teller threads to finish.
for t in teller_threads:
    t.join()

print("Bank is now closed.")
