#include <QCoreApplication>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <cmath>
#include <thread>
#include <chrono>
#include <algorithm>
#include <mutex>

using namespace std;
map<int,map<int,double>> personas;
map<pair<int,int>,double> dist;

map<int,map<int,double>> leerCSV(const std::string& archivo) {
    map<int,map<int,double>> personas;
    ifstream file(archivo);
    string linea;

    while (getline(file, linea)) {
        //qDebug()<<linea<<"\n";
        istringstream ss(linea);
        int idPersona, idPelicula;
        long long ignorar;
        double calificacion;
        char comma=','; // Variable para almacenar la coma
        ss >> idPersona >> comma >> idPelicula >> comma >> calificacion>>comma>>ignorar;
        //qDebug()<<idPersona<<" "<<idPelicula<<" "<<calificacion<<"\n";
        personas[idPersona][idPelicula]=calificacion;
    }

    return personas;
}

double distanciaManhattan(map<int,double>& v1, map<int,double>& v2) {
    double distancia = 0.0;
    for(auto & id_pelicula_calificacion : v1){
        int idPelicula=id_pelicula_calificacion.first;
        double calificacion1=id_pelicula_calificacion.second;
        double calificacion2=0;
        if(v2.count(idPelicula)){
            calificacion2=v2[idPelicula];
        }
        distancia+=abs(calificacion1-calificacion2);
    }
    return distancia;
}

double distanciaEuclidiana(map<int,double>& v1,map<int,double>& v2) {
    double distancia = 0.0;
    for(auto & id_pelicula_calificacion : v1){
        int idPelicula=id_pelicula_calificacion.first;
        double calificacion1=id_pelicula_calificacion.second;
        double calificacion2=0;
        if(v2.count(idPelicula)){
            calificacion2=v2[idPelicula];
        }
        distancia+=pow(calificacion1-calificacion2,2);
    }
    return sqrt(distancia);
}
void imprimirPersonasGustosMasCercanos(vector<pair<double,pair<int,int>>> &DistanciasMenores){
    int tamano=min(5,(int)DistanciasMenores.size());
    qDebug()<<"Las "<<tamano<< "personas con los gustos mas cercanos son : ";
    for(int i=0;i<tamano;i++){
        pair<int,int> clave=DistanciasMenores[i].second;
        double distancia=DistanciasMenores[i].first;
        qDebug()<<"Persona con Id : "<<clave.first<<" y persona con Id: "<<clave.second<<" "<<
            " con una distancia de : "<<QString::number(distancia, 'f', 7)<<"\n";
    }
}
void procesarDistancias(){
    vector<pair<double,pair<int,int>>> DistanciasMenores;
    for(auto & e : dist){
        pair<int,int> clave=e.first;
        double distancia=e.second;
        DistanciasMenores.push_back(make_pair(distancia,clave));
    }
    sort(DistanciasMenores.begin(),DistanciasMenores.end());
    imprimirPersonasGustosMasCercanos(DistanciasMenores);
}
void encontrarGustosSimilares(
    const int Idpersona) {

    //Actualiza la siguiente estructura

    // map<pair<IDpersonaA,IDpersonaB>,distancia>
    //donde IDpersonaA < IDpersonaB

    for (const auto& otraPersona : personas) {
        //Para evitar duplicados hacemos que para
        //la clave compuesta id1<id2 se cumpla
        int id1=Idpersona,id2=otraPersona.first;
        if(id1 > id2){
            swap(id1,id2);
        }
        pair<int,int> claveCompuesta=make_pair(id1,id2);
        //Si ya se calculo,lo saltamos
        if(dist.count(claveCompuesta)){
            continue;
        }
        //sino si es otra persona
        else if (Idpersona != otraPersona.first) {
            double manhattan = distanciaManhattan(personas[Idpersona], personas[otraPersona.first]);
            double euclidiana = distanciaEuclidiana(personas[Idpersona], personas[otraPersona.first]);
            //Actualizamos el mapa de distancias
            dist[claveCompuesta]=euclidiana;
        }
    }
}

// Funcion para calcular distancias en paralelo usando threads
void calcularConThreads() {
    auto start = chrono::high_resolution_clock::now();
    const int num_threads = 10;
    map<pair<int,int>,double> dist;
    // Calcular el rango de IDs mínimo y máximo
    int minId = std::numeric_limits<int>::max();
    int maxId = std::numeric_limits<int>::min();
    for (const auto& persona : personas) {
        minId = std::min(minId, persona.first);
        maxId = std::max(maxId, persona.first);
    }
    const int idRange = maxId - minId + 1;
    const std::size_t chunk_size = idRange / num_threads;
    std::vector<std::thread> threads;
    std::mutex mutex; // Declarar la variable mutex aquí

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([i, chunk_size, minId,maxId,num_threads, &personas, &dist, &mutex]() {
            int startId = minId + i * chunk_size;
            int endId = (i == num_threads - 1) ? maxId + 1 : startId + chunk_size;

            for (int idPersona = startId; idPersona < endId; ++idPersona) {
                // Función encontrarGustosSimilares con su bloque crítico
                {
                    std::lock_guard<std::mutex> lock(mutex); // Bloque crítico con lock_guard
                    encontrarGustosSimilares(idPersona);
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    procesarDistancias();
    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> duration = end - start;

    qDebug() << "Tiempo de ejecucion con threads: " << QString::number(duration.count(), 'f', 7) << " segundos\n";

}
void imprimirBaseDeDatos(){
    for(auto & id : personas){
        qDebug()<<"Persona con ID :"<<id.first<<"\ncalificaciones : ";
        for(auto & ele : id.second){
            qDebug()<<"id pelicula : "<<ele.first<<" ,rating:  "
                     <<ele.second<<"\n";
        }
        qDebug()<<"\n";
    }
}


int main(int argc, char *argv[])
{
    QCoreApplication a(argc, argv);
    auto start = chrono::high_resolution_clock::now();
    const string archivoCSV = "ratings_s.csv";
    personas = leerCSV(archivoCSV);

    //calculo sin threads
    //Realizar el calculo sin threads
    for (auto &persona : personas) {
        int idPersona=persona.first;
        encontrarGustosSimilares(idPersona);
    }
    procesarDistancias();
    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> duration = end - start;
    qDebug()<< "Tiempo de ejecucion sin threads: " <<QString::number(duration.count(), 'f', 7)<< " segundos\n";
    calcularConThreads();
    return a.exec();
}
