#include<iostream>
#include<vector>
#include<algorithm>

using namespace std;


int main(){
    int a=1234;
    int b=9876;
    int* addr_a=&a;
    int* addr_b=&b;
    int* tmp = addr_a;
    addr_a=addr_b;
    addr_b=tmp;
    std::cout<<"addr_a:"<<*addr_a<<",   addr_b:"<<*addr_b<<std::endl;
    return 0;
}