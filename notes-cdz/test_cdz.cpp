#include<iostream>
#include<vector>
#include<algorithm>

using namespace std;


int main(){
    char chs[8]={'a','b','c','e','f','g'};
    char* ptr=&chs[0];
    std::cout<<*ptr<<std::endl;
    char* ptr1=nullptr;
    return 0;
}