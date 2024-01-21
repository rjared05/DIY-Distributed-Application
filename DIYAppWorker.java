import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class DIYAppWorker
{
    Socket socket;
    String serverName;
    int portNum;

    public DIYAppWorker(String s, int p)
    {
        serverName = s;
        portNum = p;
    }

    public void run()
    {
        try
        {
            // Creating the worker socket.
            socket = new Socket(serverName, portNum);

            System.out.println("[Worker] Connection to controller established.");

            // Getting input and output streams.
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

            int sliceSize;
            BlockingQueue slice;
            double partialSum = 0;

            while(true)
            {
                // Receiving the next slice from the controller (server).
                slice = (BlockingQueue) in.readObject();

                // Checking the amount of numbers in the slice and if the slice isn't empty.
                // If it isn't empty, add all the numbers in the slice together to get its partial sum.
                // If it is empty, break out of the while loop and close the socket.
                sliceSize = 50 - slice.remainingCapacity();
                if(slice.remainingCapacity() < 50)
                    for(int i = 0; i < sliceSize; i++)
                        partialSum += Double.parseDouble(slice.take().toString());
                else
                    break;

                // Sending the partial sum back to the server.
                out.writeObject(partialSum);
                out.reset();

                // Setting the partial sum value back to 0 for the next slice.
                partialSum = 0;

                // Sleeping the server for a bit.
                Thread.sleep(1);
            }

            System.out.println("[Worker] Last partial sum has been sent.");
            System.out.println("\n[Worker] Terminating worker...");

            // Closing the socket.
            socket.close();
        }
        catch (IOException | ClassNotFoundException | InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    public static void main (String[] args)
    {
        System.out.println("Formatting: java DIYAppWorker.java server port");

        // Getting the controller (server) name and port number.
        String serverName = args[0];
        int portNum = Integer.parseInt(args[1]);

        // Creating a worker (client) object and running it.
        DIYAppWorker worker = new DIYAppWorker(serverName, portNum);
        worker.run();
    }
}