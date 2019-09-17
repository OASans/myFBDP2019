#include <mpi.h>
#include <iostream>
using namespace std;

int main(int argc, char **argv)
{
	int numtasks, rank;
	MPI_Init(&argc, &argv);
	cout<<"Hello parallel world!"<<endl;
	MPI_Finalize();
	return 0;
}
