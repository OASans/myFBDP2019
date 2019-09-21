#include <mpi.h>
#include <iostream>
#include <stdlib.h>
using namespace std;

int main(int argc, char **argv)
{
	int myid, numprocs;
	int namelen, source;
	long count = 1000000;
	
	MPI_Status status;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	
	srand((int)time(0)+myid);

	double y, x, pi = 0.0, n = 0.0;
	long m = 0, m1 = 0, i = 0, p = 0, ave_m;
	for(i = 0;i < count;i++)
	{
		x = (double)rand()/(double)RAND_MAX;
		y = (double)rand()/(double)RAND_MAX;
		if((x-0.5)*(x-0.5)+(y-0.5)*(y-0.5)<0.25)
			++m;
	}

	pi = 4.0*m/count;

	cout<<"Process "<<myid<<" of "<<numprocs<<" pi="<<pi<<endl;
	
    MPI_Reduce(&m, &ave_m, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    if(myid == 0)
        cout<<"Average pi="<<4.0*ave_m/(count*numprocs)<<endl;
    
    MPI_Finalize();
	return 0;
}
