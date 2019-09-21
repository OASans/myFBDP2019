#include <iostream>
#include <stdlib.h>
#include <time.h>
#include<mpi.h>
using namespace std;

int main(int argc, char** argv)
{
	long N = 100000000;
	int a = 0;
	int b = 10;

	int myid, numprocs;
	int i;
	double local = 0.0, dx = (double)(b-a)/N;
	double inte, x;
    MPI_Status status;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);

	for(i=myid;i<N;i=i+numprocs)
	{
		x = a+i*dx+dx/2;
		local += x*x*dx;
	}

    if(myid != 0)
        MPI_Send(&local, 1, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD);
    else
    {
        for(int source=1;source<numprocs;source++)
        {
            MPI_Recv(&inte, 1, MPI_DOUBLE, source, 1, MPI_COMM_WORLD, &status);
            local += inte;
        }
        cout<<"The integral of x*x in region ["<<a<<","<<b<<"]="<<local<<endl;
    }
    
    
    MPI_Finalize();
	return 0;
}
