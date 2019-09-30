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

	int myid, numprocs, len;
	int i;
	double local = 0.0, dx = (double)(b-a)/N;
	double inte, x;
    char hostname[MPI_MAX_PROCESSOR_NAME];

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    MPI_Get_processor_name(hostname, &len);

	for(i=myid;i<N;i=i+numprocs)
	{
		x = a+i*dx+dx/2;
		local += x*x*dx;
    }

	MPI_Reduce(&local, &inte, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
	if(myid == 0)
	{
		cout<<"MASTER: the integral of x*x in region ["<<a<<","<<b<<"]="<<inte<<endl;
	}
	MPI_Finalize();
	return 0;
}