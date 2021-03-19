/*!
* @file LWWElementDict.h
* @brief Contains CRDT LWW Element Dictionary
* @author Domagoj Markota <domagoj.markota@gmail.com>
*/

#ifndef LWWELEMENTDICT_H
#define LWWELEMENTDICT_H


#include <mutex>
#include <map>
#include <optional>


/*!
* @class LWWElementDict
* @brief CRDT Last-Write-Wins Element Dictionary
* @details CRDT LWW Element Dictionary allowing multiple insertions of same (key, value) pair.
* @tparam K key
* @tparam V value
* @tparam T timestamp
*/
template <typename K,
          typename V,
          typename T>
class LWWElementDict {
private:
    std::mutex mtx; //!< Mutual exclusion of concurrent thread execution
    std::unique_lock<std::mutex> mtxLock; //!< Locking mechanism providing secure insertion from multiple threads.

    std::map<K, std::multimap<V, T>> addedData; //!< CRDT added elements
    std::map<K, std::multimap<V, T>> removedData; //!< CRDT removed elements
    std::map<K, std::pair<V, T>> currentData; //!< CRDT current elements


public:
    /*!
    * Default constructor
    */
    LWWElementDict();


    /*!
    * Copy constructor
    * @param[in] dict Source dictionary
    */
    LWWElementDict(const LWWElementDict & dict);


    /*!
    * Default virtual destructor
    */
    virtual ~LWWElementDict() = default;


    /*!
    * Insert new element into \a addedData map in less order
    * @param [in] k key
    * @param [in] v value
    * @param [in] t timestamp
    */
    virtual void addElement(const K & k, const V & v, const T & t);


    /*!
    * Insert new element into \a removedData map in less order
    * @param [in] k key
    * @param [in] v value
    * @param [in] t timestamp
    */
    virtual void removeElement(const K & k, const V & v, const T & t);


    /*!
    * Invoking addElement method
    * @param [in] k key
    * @param [in] v value
    * @param [in] t timestamp
    */
    virtual void updateValue(const K & k, const V & v, const T & t);


    /*!
    * Retrieving current value for specified map's key \p k .
    * @param [in] k key
    * @return container with corresponding value if exists, empty otherwise
    * @retval std::optional<V> value type within std::optional container
    */
    virtual const std::optional<const V> getValueByKey(const K & k);


    /*!
    * Adding elements from \p dict 's maps to maps of this instance while avoiding duplicates and preserving less order.
    * @param [in] dict Source dictionary
    */
    virtual void mergeWith(const LWWElementDict & dict);


private:
    /*!
    * Fetching time from latest removal request for specified key \p k .
    * @param [in] k key
    * @return time for latest removal request
    * @retval std::optional<T> timestamp type within std::optional container
    */
    const std::optional<const T> getLastRemovalTime(const K & k);


    /*!
    * Register the element as currently contained.
    * @param [in] k key
    * @param [in] v value
    * @param [in] t timestamp
    */
    void addToCurrentData(const K & k, const V & v, const T & t);


    /*!
    * Unregister the element as currently contained.
    * @param [in] k key
    * @param [in] v value
    * @param [in] t timestamp
    */
    void removeFromCurrentData(const K & k, const V & v, const T & t);


    /*!
    * Less-ordered insertion
    * @param [in,out] multimap Target container
    * @param [in] pair Data to insert
    */
    void orderedInsert(std::multimap<V, T> & multimap, const std::pair<V, T> & pair);


    /*!
    * Adding elements from \p dataSrc to \p dataDest while avoiding duplicates and preserving less order. Updates \a currentData.
    * @param [in] addFlag If true, elements are added to \a currentData , otherwise removed.
    * @param [in,out] dataDest Merging destination
    * @param [in] dataSrc Merging source
    */
    void mergeData(
        const bool & addFlag,
        std::map<K, std::multimap<V, T>> & dataDest,
        const std::map<K, std::multimap<V, T>> & dataSrc
    );


public:
    const auto & getAddedData() const;
    const auto & getRemovedData() const;
    const auto & getCurrentData() const;

};



template <typename K, typename V, typename T>
LWWElementDict<K, V, T>::LWWElementDict(
):
    mtxLock(std::unique_lock<std::mutex>(this->mtx, std::defer_lock))
{
}



template <typename K, typename V, typename T>
LWWElementDict<K, V, T>::LWWElementDict(
    const LWWElementDict & dict
):
    mtxLock(std::unique_lock<std::mutex>(this->mtx, std::defer_lock)),

    addedData(dict.getAddedData()),
    removedData(dict.getRemovedData())
{
}



template <typename K, typename V, typename T>
void LWWElementDict<K, V, T>::addElement(const K & k, const V & v, const T & t)  {
    this->mtxLock.lock();
    this->orderedInsert(this->addedData[k], { v, t });
    this->addToCurrentData(k, v, t);
    this->mtxLock.unlock();
}



template <typename K, typename V, typename T>
void LWWElementDict<K, V, T>::removeElement(const K & k, const V & v, const T & t)  {
    this->mtxLock.lock();
    this->orderedInsert(this->removedData[k], { v, t });
    this->removeFromCurrentData(k, v, t);
    this->mtxLock.unlock();
}



template <typename K, typename V, typename T>
void LWWElementDict<K, V, T>::updateValue(const K & k, const V & v, const T & t) {
    this->addElement(k, v, t);
}



template <typename K, typename V, typename T>
const std::optional<const V> LWWElementDict<K, V, T>::getValueByKey(const K & k) {
    if(this->currentData.find(k) != this->currentData.end()) {
        return { this->currentData[k].first };
    } else {
        return {};
    }
}



template <typename K, typename V, typename T>
void LWWElementDict<K, V, T>::mergeWith(const LWWElementDict & dict) {
    this->mtxLock.lock();
    this->mergeData(true, this->addedData, dict.getAddedData());
    this->mergeData(false, this->removedData, dict.getRemovedData());
    this->mtxLock.unlock();
}



template <typename K, typename V, typename T>
const std::optional<const T> LWWElementDict<K, V, T>::getLastRemovalTime(const K & k) {
    if(this->removedData.find(k) != this->removedData.end()) {
        for(const auto & [v, t] : this->removedData[k]) {
            //
        }
    } else {
        return {};
    }
}



template <typename K, typename V, typename T>
void LWWElementDict<K, V, T>::addToCurrentData(const K & k, const V & v, const T & t) {
    const auto timeCont = this->getLastRemovalTime(k);
    if(timeCont) {
        // If element's timestamps for insertion and removal are the same, then removal has priority.
        if(*timeCont < t) {
            return;
        }
    }

    if(this->currentData.find(k) == this->currentData.end()) {
        this->currentData[k] = { v, t };
    } else {
        if(t > this->currentData[k].second) {
            this->currentData[k] = { v, t };
        }
    }
}



template <typename K, typename V, typename T>
void LWWElementDict<K, V, T>::removeFromCurrentData(const K & k, const V & v, const T & t) {
    if(this->currentData.find(k) != this->currentData.end()) {
        // If element's timestamps for insertion and removal are the same, then removal has priority.
        if(t >= this->currentData[k].second) {
            this->currentData.erase(k);
        }
    }
}



template <typename K, typename V, typename T>
void LWWElementDict<K, V, T>::orderedInsert(
    std::multimap<V, T> & multimap,
    const std::pair<V, T> & pair
) {
    const auto multimapRange = multimap.equal_range(pair.first);

    if(multimapRange.first == multimapRange.second) {
        multimap.insert(pair);
    } else {
        bool insertFlag = true;

        for(auto multimapIter = multimapRange.first; multimapIter != multimapRange.second; ++multimapIter) {
            if(pair.second < multimapIter->second) {
                multimap.insert(multimapIter, pair);
                insertFlag = false;
                break;
            } else if(pair.second == multimapIter->second) {
                insertFlag = false;
                break;
            }
        }

        if(insertFlag) {
            multimap.insert(multimapRange.second, pair);
        }
    }
}



template <typename K, typename V, typename T>
void LWWElementDict<K, V, T>::mergeData(
    const bool & addFlag,
    std::map<K, std::multimap<V, T>> & dataDest,
    const std::map<K, std::multimap<V, T>> & dataSrc
) {
    for(const auto & [keySrc, multimapSrc] : dataSrc) {
        auto mapIterDest = dataDest.find(keySrc);

        if(mapIterDest == dataDest.end()) {
            dataDest[keySrc] = multimapSrc;
        } else {
            for(auto multimapIterSrc = multimapSrc.begin(); multimapIterSrc != multimapSrc.end(); ++multimapIterSrc) {
                this->orderedInsert(mapIterDest->second, *multimapIterSrc);
                
                if(addFlag) {
                    this->addToCurrentData(keySrc, multimapIterSrc->first, multimapIterSrc->second);
                } else {
                    this->removeFromCurrentData(keySrc, multimapIterSrc->first, multimapIterSrc->second);
                }
            }
        }
    }
}



template <typename K, typename V, typename T>
const auto & LWWElementDict<K, V, T>::getAddedData() const {
    return this->addedData;
}



template <typename K, typename V, typename T>
const auto & LWWElementDict<K, V, T>::getRemovedData() const {
    return this->removedData;
}



template <typename K, typename V, typename T>
const auto & LWWElementDict<K, V, T>::getCurrentData() const {
    return this->currentData;
}



#endif // LWWELEMENTDICT_H
