# SDpract2

Versió distribuïda d'un algorisme d'exclusió mútua centralitzat, que controla que exactament una funció "cloud" pot entrar a una secció crítica en cada moment. En aquest cas els fitxer compartits es guarden a IBM COS. La tecnologia d'implementació utilitzada és IBM-PyWren. 
La implementació conté dues funcions, una master i una slave. La master s'encarrega de dirigir les peticions d'escriptura dels slaves. 
