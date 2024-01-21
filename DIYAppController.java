import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class DIYAppController extends Thread
{
    public static void main (String[] args) throws IOException
    {
        System.out.println("Formatting: java DIYAppController.java port");

        // Value that keeps track of each worker.
        int workerNum = 0;

        // Making a new partition object.
        partition partitioning = new partition();

        // Creating the controller socket.
        int portNum = Integer.parseInt(args[0]);
        ServerSocket controllerSocket = new ServerSocket(portNum);

        System.out.println("[Controller] Waiting for a worker...");

        while (true)
        {
            Socket socket = controllerSocket.accept();

            System.out.println("[Controller] Accepting a new worker...");

            // Adding on to how many workers (threads) there are currently and
            // using the value to keep track of the current worker.
            partitioning.threadCount++;
            workerNum = partitioning.threadCount;

            System.out.println("[Controller] New worker (Worker " + workerNum + ") accepted.");
            if(partitioning.threadCount - 1 == 0)
                System.out.println("[Controller] Starting the addition process!");

            // Making a new thread object and starting it, so it handles the worker.
            DIYAppWorkerHandler workerThread = new DIYAppWorkerHandler(socket, partitioning, workerNum);
            new Thread(workerThread).start();

                // Avoiding unreachable statement error...
                if(controllerSocket == null)
                    break;
        }

        // Closing the socket.
        controllerSocket.close();
    }
}

// Class dedicated to partitioning the data file.
class partition
{
    double finalSum = 0;

    static String line;
    static BlockingQueue<Double> slice = new LinkedBlockingDeque<>(50);
    BlockingQueue<BlockingQueue> bigDataStore;
    BlockingQueue emptySlice = new LinkedBlockingDeque(50);

    int threadCount = 0;

    public partition() throws FileNotFoundException
    {
        // Creating a buffered reader to read each number in the data file.
        String path = new File("").getAbsolutePath();
        File file = new File(path + "\\test.dat");
        BufferedReader read = new BufferedReader(new InputStreamReader(new FileInputStream(file)));

        // Creating and running a thread that constantly reads numbers from
        // the data file, separating them into 50-number slices.
        // These numbers are put into blocking queues (slices) that are put into
        // a larger blocking queue to be used by the workers.
        bigDataStore = new LinkedBlockingDeque<>(100);
        Thread addNewData = new Thread(() ->
        {
            try
            {
                // Reading the next number from the data file.
                line = read.readLine();
                while(true)
                {
                    if(line != null)
                    {
                        // Adds 50 numbers from the data file to a slice if there is space in
                        // the larger blocking queue that holds all the slices that are ready to be sent to workers.
                        if(bigDataStore.remainingCapacity() != 0)
                        {
                            for(int i = 0; i < 50; i++)
                            {
                                if(line != null)
                                {
                                    slice.put(Double.parseDouble(line));
                                    line = read.readLine();
                                }
                                else
                                {
                                    System.out.println("[Controller] Entire data file has been partitioned into slices.");

                                    break;
                                }
                            }

                            // Putting the slice into the larger blocking queue.
                            bigDataStore.put(slice);

                            // Breaks out of the loop if there are no more numbers in the file.
                            if(line == null)
                                break;

                            // Resets the slice value so a new set of numbers can be inserted into it.
                            slice = new LinkedBlockingDeque<>(50);
                        }
                    }
                    else
                    {
                        System.out.println("[Controller] Entire data file has been partitioned into slices.");

                        break;
                    }
                }
            }
            catch (IOException | InterruptedException e)
            {
                e.printStackTrace();
            }
        } );
        addNewData.start();
    }

    // Synchronized method that returns the next slice in the larger blocking queue.
    public synchronized Object getNextSlice() throws InterruptedException
    {
        return bigDataStore.take();
    }

    // Synchronized method that adds a partial sum from a worker into the final sum.
    public synchronized void addFinalSum(double workerPartial)
    {
        finalSum += workerPartial;
    }
}

// Class dedicated to handling the worker (thread) on the controller (server).
class DIYAppWorkerHandler implements Runnable
{
    Socket workerSocket;
    ObjectOutputStream out;
    ObjectInputStream in;
    partition partitioning;

    int workerNum;

    public DIYAppWorkerHandler(Socket socket, partition partitioning, int workerNum) throws IOException
    {
        workerSocket = socket;
        this.partitioning = partitioning;
        this.workerNum = workerNum;

        out = new ObjectOutputStream(workerSocket.getOutputStream());
        in = new ObjectInputStream(workerSocket.getInputStream());
    }

    public void run()
    {
        try
        {
            while(true)
            {
                // Serializing a slice of data and sending it to a worker.
                // If there are no slices left, an empty slice is sent to the worker.
                if(partitioning.bigDataStore.remainingCapacity() < 100)
                    out.writeObject(partitioning.getNextSlice());
                else
                {
                    System.out.println("[Worker " + workerNum + "] Last partial sum has been sent.");

                    out.writeObject(partitioning.emptySlice);
                    break;
                }
                out.reset();

                // Receiving the partial sum from the worker and adding it to the final sum.
                partitioning.addFinalSum(Double.parseDouble(in.readObject().toString()));
            }

            System.out.println("[Controller] Terminating Worker " + workerNum + "...");

            // Decreasing the worker (thread) count and terminating the server if there are no more workers currently running.
            partitioning.threadCount--;

            if(partitioning.threadCount == 0)
            {
                System.out.println("\n[Controller] The final sum is " + partitioning.finalSum + ".");
                System.out.println("[Controller] Terminating controller...");

                System.exit(0);
            }
        }
        catch (IOException | InterruptedException | ClassNotFoundException e)
        {
            e.printStackTrace();
        }
    }
}