#include<iostream>
#include<vector>
#include<algorithm>

using namespace std;


//降序
bool compare(int a,int b){
	return a-b;
}

int main(){

    // vector<int> arr={5,2,1,4,3};    
    vector<int> arr={2,5}; 
    sort(arr.begin(),arr.end(),compare);
    for(int i=0;i<arr.size();i++){
        cout<<arr[i]<<" ";
    }
    return 0;
}